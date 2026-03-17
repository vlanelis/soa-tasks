from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import grpc


def _should_retry(exc):
    if isinstance(exc, grpc.RpcError):
        code = exc.code()
        return code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
    return False


def retry_on_unavailable(fn):
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.1), retry=retry_if_exception(lambda e: _should_retry(e)))
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapped
