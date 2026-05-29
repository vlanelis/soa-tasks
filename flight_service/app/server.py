import grpc
from concurrent import futures
from datetime import datetime
import logging
import threading

from grpc_health.v1 import health_pb2, health_pb2_grpc

from prometheus_client import start_http_server

from sqlalchemy.orm import Session
from sqlalchemy import and_

from .database import SessionLocal
from .models import Flight, SeatReservation, FlightStatus, ReservationStatus
from .cache import (
    get_flight, set_flight, invalidate_flight,
    get_search, set_search, invalidate_search
)
from .auth import ApiKeyInterceptor
from .metrics import PrometheusInterceptor

import flight_pb2
import flight_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flight_service")


def serialize_flight(f: Flight):
    return {
        "id": str(f.id),
        "flight_number": f.flight_number,
        "origin": f.origin,
        "destination": f.destination,
        "departure_time": f.departure_time.isoformat() if f.departure_time else None,
        "arrival_time": f.arrival_time.isoformat() if f.arrival_time else None,
        "total_seats": f.total_seats,
        "available_seats": f.available_seats,
        "price": float(f.price),
        "status": f.status.value
    }


def restore_cached_flight(f: dict):
    if f.get("departure_time"):
        f["departure_time"] = datetime.fromisoformat(f["departure_time"])
    if f.get("arrival_time"):
        f["arrival_time"] = datetime.fromisoformat(f["arrival_time"])


class FlightService(flight_pb2_grpc.FlightServiceServicer):
    def GetFlight(self, request, context):
        cached = get_flight(request.id)

        if cached:
            restore_cached_flight(cached["flight"])
            logger.info("Flight cache hit: %s", request.id)
            return flight_pb2.FlightResponse(flight=cached["flight"])

        logger.info("Flight cache miss: %s", request.id)
        db: Session = SessionLocal()

        try:
            flight = db.get(Flight, request.id)

            if not flight:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
                return

            data = {"flight": serialize_flight(flight)}

            set_flight(request.id, data)

            restore_cached_flight(data["flight"])

            return flight_pb2.FlightResponse(**data)
        finally:
            db.close()

    def SearchFlights(self, request, context):
        date = request.date.ToDatetime().strftime("%Y-%m-%d")
        if date == "1970-01-01":
            date = None

        cached = get_search(
            request.origin,
            request.destination,
            date
        )

        if cached:
            logger.info("Search cache hit: %s %s %s", request.origin, request.destination, date)
            for flight in cached["flights"]:
                restore_cached_flight(flight)
            return flight_pb2.SearchFlightsResponse(
                flights=cached["flights"]
            )
        logger.info("Search cache miss: %s %s %s", request.origin, request.destination, date)

        db: Session = SessionLocal()

        try:
            if date:
                start = datetime.strptime(date, "%Y-%m-%d")
                end = start.replace(hour=23, minute=59, second=59)

                flights = db.query(Flight).filter(
                    and_(
                        Flight.origin == request.origin,
                        Flight.destination == request.destination,
                        Flight.departure_time >= start,
                        Flight.departure_time <= end,
                        Flight.status == FlightStatus.SCHEDULED
                    )
                ).all()
            else:
                flights = db.query(Flight).filter(
                    and_(
                        Flight.origin == request.origin,
                        Flight.destination == request.destination,
                        Flight.status == FlightStatus.SCHEDULED
                    )
                ).all()

            result = [serialize_flight(f) for f in flights]

            data = {"flights": result}

            set_search(
                request.origin,
                request.destination,
                date,
                data
            )
            for flight in data["flights"]:
                restore_cached_flight(flight)

            return flight_pb2.SearchFlightsResponse(flights=result)
        finally:
            db.close()

    def ReserveSeats(self, request, context):
        db: Session = SessionLocal()

        try:
            existing = db.query(SeatReservation).filter_by(
                booking_id=request.booking_id
            ).first()

            if existing:
                return flight_pb2.ReserveSeatsResponse(
                    reservation_id=str(existing.id)
                )

            flight = db.query(Flight)\
                .with_for_update()\
                .get(request.flight_id)

            if not flight:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
                return

            if flight.status != FlightStatus.SCHEDULED:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "flight not active")
                return

            if flight.available_seats < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough seats")
                return

            flight.available_seats -= request.seat_count

            reservation = SeatReservation(
                flight_id=request.flight_id,
                booking_id=request.booking_id,
                seat_count=request.seat_count,
                status=ReservationStatus.ACTIVE
            )

            db.add(reservation)
            db.commit()

            invalidate_flight(request.flight_id)
            invalidate_search(
                flight.origin,
                flight.destination,
                flight.departure_time.date().isoformat()
            )

            return flight_pb2.ReserveSeatsResponse(
                reservation_id=str(reservation.id)
            )

        finally:
            db.close()

    def ReleaseReservation(self, request, context):
        db = SessionLocal()
        try:
            reservation = (
                db.query(SeatReservation)
                .filter_by(booking_id=request.booking_id)
                .with_for_update()
                .first()
            )
            if not reservation:
                context.abort(grpc.StatusCode.NOT_FOUND, "reservation not found")
                return

            if reservation.status != ReservationStatus.ACTIVE:
                return flight_pb2.ReleaseReservationResponse(success=True)

            flight = db.query(Flight).with_for_update().get(reservation.flight_id)
            flight.available_seats += reservation.seat_count
            reservation.status = ReservationStatus.RELEASED
            db.commit()

            invalidate_flight(str(flight.id))
            invalidate_search(
                flight.origin,
                flight.destination,
                flight.departure_time.date().isoformat()
            )
            return flight_pb2.ReleaseReservationResponse(success=True)
        finally:
            db.close()


class HealthServicer(health_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)


def serve_metrics():
    start_http_server(8001)   # порт для метрик


def serve():
    threading.Thread(target=serve_metrics, daemon=True).start()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[ApiKeyInterceptor(), PrometheusInterceptor()]
    )

    flight_pb2_grpc.add_FlightServiceServicer_to_server(FlightService(), server)
    health_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)

    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
