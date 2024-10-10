import email.utils as eut
import logging
import time
from contextlib import contextmanager
import firecrest.FirecrestException as fe


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
        log_func(
            logging.WARNING,
            f"Could not parse Retry-After header: {retry_after_header}"
        )
        return 10


def validate_api_version_compatibility(**expected_flags):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            missing_features = missing_api_features.get(self._api_version, {}).get(func.__name__, [])

            if 'ALL' in missing_features:
                raise fe.NotImplementedOnAPIversion(f"All features for {func.__name__}"
                                                    " are not developed yet for the current API version.")

            for flag, value in expected_flags.items():
                if kwargs.get(flag) == value and flag in missing_features:
                    raise fe.NotImplementedOnAPIversion(f"The flag {flag}={value} is not developed"
                                                        " yet for {func.__name__} for the current API version.")

            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def async_validate_api_version_compatibility(**expected_flags):
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            missing_features = missing_api_features.get(self._api_version, {}).get(func.__name__, [])

            if 'ALL' in missing_features:
                raise fe.NotImplementedOnAPIversion(f"All features for {func.__name__} are "
                                                    "not developed yet for the current API version.")

            for flag, value in expected_flags.items():
                if kwargs.get(flag) == value and flag in missing_features:
                    raise fe.NotImplementedOnAPIversion(f"The flag {flag}={value} is not developed"
                                                        " yet for {func.__name__} for the current API version.")

            return await func(self, *args, **kwargs)
        return wrapper
    return decorator


missing_api_features = {
    '1.15.0': {
        'list_files': ['recursive'],
        'compress': ['ALL'],
        'extract': ['ALL'],
        'submit_compress_job': ['ALL'],
        'submit_extract_job': ['ALL']
    },
    '1.16.0': {},
}
