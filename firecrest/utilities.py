import logging
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


def slurm_state_completed(state):
    completion_states = {
        'BOOT_FAIL',
        'CANCELLED',
        'COMPLETED',
        'DEADLINE',
        'FAILED',
        'NODE_FAIL',
        'OUT_OF_MEMORY',
        'PREEMPTED',
        'TIMEOUT',
    }
    if state:
        return all(s in completion_states for s in state.split(','))

    return False


def parse_retry_after(retry_after_header, log_func):
    """
    Parse the Retry-After header.

    :param retry_after_header: The value of the Retry-After header.
    :return: A non-negative floating point number representing when the retry
    should occur.
    """
    try:
        # Try to parse it as a delta-seconds
        delta_seconds = int(retry_after_header)
        return max(delta_seconds, 0)
    except ValueError:
        pass

    try:
        # Try to parse it as an HTTP-date
        retry_after_date = eut.parsedate_to_datetime(retry_after_header)
        delta_seconds = retry_after_date.timestamp() - time.time()
        return max(delta_seconds, 0)
    except Exception:
        log_func(logging.WARNING, f"Could not parse Retry-After header: {retry_after_header}")
        return 10
