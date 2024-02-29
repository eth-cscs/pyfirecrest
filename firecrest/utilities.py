import time
from contextlib import contextmanager

@contextmanager
def time_block(label, logger):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        logger.debug(f"{label} took {end_time - start_time:.6f} seconds")
