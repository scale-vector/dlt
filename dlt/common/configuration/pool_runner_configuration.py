from typing import Literal, Optional
from dlt.common.configuration import RunConfiguration

TPoolType = Literal["process", "thread", "none"]

class PoolRunnerConfiguration(RunConfiguration):
    WORKERS: Optional[int] = None  # how many threads/processes in the pool
    EXIT_ON_EXCEPTION: bool = False  # should exit on exception
    STOP_AFTER_RUNS: int = 10000  # will stop runner with exit code -2 after so many runs, that prevents memory fragmentation
    POOL_TYPE: TPoolType = None  # type of pool to run, must be set in derived configs
    RUN_SLEEP: float = 0.5  # how long to sleep between runs with workload, seconds
    RUN_SLEEP_IDLE: float = 1.0  # how long to sleep when no more items are pending, seconds
    RUN_SLEEP_WHEN_FAILED: float = 1.0  # how long to sleep between the runs when failed
