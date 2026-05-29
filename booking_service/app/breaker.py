import pybreaker
import logging
import grpc

from .config import BREAKER_FAIL_MAX, BREAKER_RESET_TIMEOUT


logger = logging.getLogger("breaker")


class LogListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        msg = "State Change: Old State: {0}, New State: {1}".format(old_state, new_state)
        logger.info(msg)


def is_client_error(exception):
    if isinstance(exception, grpc.RpcError):
        grpc_code = exception.code()

        client_errors = [
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        ]
        if grpc_code in client_errors:
            return True

    return False


breaker = pybreaker.CircuitBreaker(
    fail_max=BREAKER_FAIL_MAX,
    reset_timeout=BREAKER_RESET_TIMEOUT,
    listeners=[LogListener()],
    exclude=[is_client_error]
)
