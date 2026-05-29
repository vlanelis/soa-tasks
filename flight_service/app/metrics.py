import time
import grpc
from prometheus_client import Counter, Histogram

REQUEST_COUNT = Counter('grpc_requests_total', 'Total requests', ['method', 'status'])
ERROR_COUNT = Counter('grpc_errors_total', 'Total errors', ['method', 'error_type'])
REQUEST_DURATION = Histogram('grpc_request_duration_seconds', 'Request duration', ['method'])

class PrometheusInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method

        handler = continuation(handler_call_details)

        if handler.request_streaming or handler.response_streaming:
            return handler

        def wrapper(request, context):
            start = time.perf_counter()
            try:
                response = handler.unary_unary(request, context)
                REQUEST_COUNT.labels(method=method, status='ok').inc()
                return response
            except Exception as e:
                if hasattr(e, 'code'):
                    error_type = e.code().name
                else:
                    error_type = type(e).__name__
                REQUEST_COUNT.labels(method=method, status='error').inc()
                ERROR_COUNT.labels(method=method, error_type=error_type).inc()
                raise
            finally:
                duration = time.perf_counter() - start
                REQUEST_DURATION.labels(method=method).observe(duration)

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer
        )