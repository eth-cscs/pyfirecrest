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
