from typing import Any, Iterator, Union, cast, Sequence
from hexbytes import HexBytes
import requests

from dlt.common.typing import DictStrAny
from dlt.common.sources import TDeferred, TItem, defer_iterator, with_retry

from dlt.pipeline.exceptions import MissingDependencyException

try:
    # import gracefully and produce nice exception that explains the user what to do
    from web3 import Web3, HTTPProvider
except ImportError:
    raise MissingDependencyException("Ethereum Source", ["web3"], "Web3 is a all purpose python library to interact with Ethereum-compatible blockchains.")

# this source gets full state of any Ethereum compatible blockchain.
# - can be easily scaled up to hundreds of parallel extractions (ie. via Airflow)
# - supports retrying if node fails
# - supports multi threaded extraction on single machine via get_deferred_source
# - supports pipeline state so it may be used to just get new blocks when they are available
# - the return value is iterator (or deferred iterator) so still mappings may be used (ie. to decode transactions and logs), see example


def get_source(node_url: str, no_blocks: int, last_block: int = None, lag: int = 2, state: DictStrAny = None) -> Iterator[DictStrAny]:
    return _get_source(False, node_url, no_blocks, last_block, lag, state)  # type: ignore


def get_deferred_source(node_url: str, no_blocks: int, last_block: int = None, lag: int = 2, state: DictStrAny = None) -> Iterator[TDeferred[DictStrAny]]:
    return _get_source(True, node_url, no_blocks, last_block, lag, state)  # type: ignore


def _get_source(is_deferred: bool, node_url: str, no_blocks: int, last_block: int, lag: int, state: DictStrAny = None) -> Union[Iterator[TItem], Iterator[TDeferred[DictStrAny]]]:

    # this code is run only once
    w3 = Web3(Web3.HTTPProvider(node_url))

    # last block is not provided then take the highest block from the chain
    if last_block is None:
        # 14788082
        last_block = w3.eth.get_block_number() - lag

    # get default current block
    current_block = last_block - no_blocks + 1

    # get current block from the state if available
    if state:
        current_block = state.get("ethereum_current_block", current_block)

    assert current_block >= 0
    assert current_block <= last_block

    # get chain id
    chain_id = w3.eth.chain_id

    # do not get highest block (in fact we should have 3 block lag here)
    while current_block <= last_block:
        # get rid of AttributeDict (web3 didn't adopt TypedDict)
        print(f"requesting block {current_block}")

        @defer_iterator
        @with_retry()
        def _get_block_deferred() -> DictStrAny:
            return _get_block(w3, current_block, chain_id)

        @with_retry()
        def _get_block_retry() -> DictStrAny:
            return _get_block(w3, current_block, chain_id)

        # yield deferred items or actual item values
        if is_deferred:
            yield _get_block_deferred()
        else:
            yield _get_block_retry()
        current_block += 1

    # update state, it will be read and stored with the pipeline instance ONLY when whole iterator finishes
    # state participates in the same atomic operation as data
    # this must happen after last yield
    print(f"finalizing {state}")
    if state is not None:
        state["ethereum_current_block"] = current_block


def _get_block(w3: Web3, current_block: int, chain_id: int) -> DictStrAny:
    print(f"producing block {current_block}")

    # get block with all transaction
    block = dict(w3.eth.get_block(current_block, full_transactions=True))
    # set explicit chain id
    block["chain_id"] = chain_id
    # get rid of AttributeDict (web3 didn't adopt TypedDict)
    attr_txs = cast(Sequence[Any], block["transactions"])
    transactions: Sequence[DictStrAny] = [dict(tx) for tx in attr_txs]
    for tx in transactions:
        if "accessList" in tx and len(tx["accessList"]) > 0:
            tx["accessList"] = [dict(al) for al in tx["accessList"]]
        # propagate sorting and clustering info (we could also do it in normalize: TODO:!)
        tx["block_timestamp"] = block["timestamp"]

        # overwrite chain_id which is not provided in all cases
        tx["chainId"] = chain_id

    block["transactions"] = transactions
    block["logsBloom"] = bytes(cast(HexBytes, block["logsBloom"]))  # serialize as bytes

    # get transaction receipts using batching. web3 does not support batching so we must
    # call node directly and then convert hex numbers to ints
    batch = []
    for idx, tx in enumerate(transactions):
        batch.append({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx["hash"]],
            "id": idx
        })
    provider = cast(HTTPProvider, w3.provider)
    r = requests.post(provider.endpoint_uri, json=batch, timeout=(20, 12))
    r.raise_for_status()
    receipts = r.json()
    for tx_receipt, tx in zip(receipts, transactions):
        if "result" not in tx_receipt:
            raise ValueError(tx_receipt)
        tx_receipt = tx_receipt["result"]
        assert tx_receipt["transactionHash"] == tx["hash"].hex()
        tx["transactionIndex"] = Web3.toInt(hexstr=tx_receipt["transactionIndex"])
        tx["status"] = Web3.toInt(hexstr=tx_receipt["status"])
        tx["logs"] = tx_receipt["logs"]
        for log in tx_receipt["logs"]:
            log["blockNumber"] = Web3.toInt(hexstr=log["blockNumber"])
            log["transactionIndex"] = Web3.toInt(hexstr=log["transactionIndex"])
            log["logIndex"] = Web3.toInt(hexstr=log["logIndex"])
            log["block_timestamp"] = block["timestamp"]
            log["block_hash"] = block["hash"]

    return block
