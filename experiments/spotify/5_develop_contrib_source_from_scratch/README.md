# Developing a contrib source from scratch

We develop new source with the intention to be importable. Probably we are a software developer or an agency and our source is something really popular ie. Hubspot. The workflow is not that different from `develop contrib source from own source` but:

1. The source structure would be much cleaner and decorators will be used, not `as_table`
2. The schema proably won't be hardcoded in a file, just the key elements will be passed as hints - the exact structure will be inferred.
3. We could let the used to select the tables if there are mutiple schemas
4. It would follow some good practices to write robust code. The one most important that comes to my mind is to

- provide retry mechanism on api calls
- make api calls (or anything waiting on i/o) to work async or in parallel

The above can be done with a single decorator and also forces a clean code. Example

```python
# download a range of ethereum blocks and yield their transactions
for block_no in range(blocks):
    block = web3.get_block(block_no)
    yield from as_table(block["transactions], table_name="transactions", write_disposition="append")
```

should rather work like this

```python
@deferred
@retry(max=10)
@table(write_disposition="append")
def transactions(block_no):
    return web3.get_block(block_no)["transactions]

for block_no in range(blocks):
    yield from transactions(block_no)
```

or when using async
```python
@retry(max=10)
@table(write_disposition="append")
async def transactions(block_no):
    return await web3.async_get_block(block_no)["transactions]

for block_no in range(blocks):
    yield from transactions(block_no)
```
