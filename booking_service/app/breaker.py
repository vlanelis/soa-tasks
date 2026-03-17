import pybreaker
import logging

from .config import BREAKER_FAIL_MAX, BREAKER_RESET_TIMEOUT, BREAKER_RECOVER_TIMEOUT


logger = logging.getLogger("breaker")


class LogListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        msg = "State Change: Old State: {0}, New State: {1}".format(old_state, new_state)
        logger.info(msg)


breaker = pybreaker.CircuitBreaker(
    fail_max=BREAKER_FAIL_MAX,
    reset_timeout=BREAKER_RESET_TIMEOUT,
    recover_timeout=BREAKER_RECOVER_TIMEOUT,
    listeners=[LogListener()]
)
