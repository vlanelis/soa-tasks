from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
import time

from starlette.routing import Match

REQUESTS = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
ERRORS = Counter('http_request_errors_total', 'HTTP errors', ['method', 'endpoint', 'error_type'])
DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration', ['method', 'endpoint'])


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        endpoint = request.url.path
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                endpoint = route.path
                break

        method = request.method
        start = time.perf_counter()
        try:
            response = await call_next(request)
            status = response.status_code
            REQUESTS.labels(method=method, endpoint=endpoint, status=status).inc()
            if status >= 400:
                ERRORS.labels(method=method, endpoint=endpoint, error_type=str(status)).inc()
        except Exception as e:
            ERRORS.labels(method=method, endpoint=endpoint, error_type=type(e).__name__).inc()
            raise
        finally:
            duration = time.perf_counter() - start
            DURATION.labels(method=method, endpoint=endpoint).observe(duration)
        return response
