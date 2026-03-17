import grpc
import logging
from .config import FLIGHT_GRPC_TARGET, SERVICE_API_KEY
import flight_pb2
import flight_pb2_grpc
from .retry_policy import retry_on_unavailable
from .breaker import breaker

logger = logging.getLogger("grpc_client")
_channel = grpc.insecure_channel(FLIGHT_GRPC_TARGET)
_stub = flight_pb2_grpc.FlightServiceStub(_channel)
_metadata = (("x-api-key", SERVICE_API_KEY),)

def _wrap_rpc(rpc_call, *args, **kwargs):
    try:
        return rpc_call(*args, **kwargs)
    except grpc.RpcError as e:
        logger.exception("gRPC error: %s", e)
        raise

@breaker
@retry_on_unavailable
def get_flight(flight_id):
    req = flight_pb2.GetFlightRequest(id=flight_id)
    return _wrap_rpc(_stub.GetFlight, req, metadata=_metadata)

@breaker
@retry_on_unavailable
def search_flights(origin, destination, date_ts=None):
    req = flight_pb2.SearchFlightsRequest(origin=origin, destination=destination)
    if date_ts:
        from google.protobuf.timestamp_pb2 import Timestamp
        t = Timestamp()
        t.FromSeconds(int(date_ts))
        req.date.CopyFrom(t)
    return _wrap_rpc(_stub.SearchFlights, req, metadata=_metadata)

@breaker
@retry_on_unavailable
def reserve_seats(flight_id, booking_id, seat_count):
    req = flight_pb2.ReserveSeatsRequest(flight_id=flight_id, booking_id=booking_id, seat_count=seat_count)
    return _wrap_rpc(_stub.ReserveSeats, req, metadata=_metadata)

@breaker
@retry_on_unavailable
def release_reservation(booking_id):
    req = flight_pb2.ReleaseReservationRequest(booking_id=booking_id)
    return _wrap_rpc(_stub.ReleaseReservation, req, metadata=_metadata)
