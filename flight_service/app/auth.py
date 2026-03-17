import grpc
from .config import SERVICE_API_KEY


class ApiKeyInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        meta = {}
        if handler_call_details.invocation_metadata:
            for k, v in handler_call_details.invocation_metadata:
                meta[k] = v
        key = meta.get("x-api-key")
        if key != SERVICE_API_KEY:
            def unauth(request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")
            return grpc.unary_unary_rpc_method_handler(unauth)
        return continuation(handler_call_details)
