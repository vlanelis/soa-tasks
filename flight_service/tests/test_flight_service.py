import pytest
from unittest.mock import MagicMock, Mock, patch, ANY
from datetime import datetime
import grpc

from app.server import FlightService
from app.models import Flight, SeatReservation, FlightStatus, ReservationStatus
import flight_pb2
from google.protobuf.timestamp_pb2 import Timestamp


@pytest.fixture
def mock_db_session():
    with patch("app.server.SessionLocal") as mock:
        session = Mock()
        mock.return_value = session
        yield session


@pytest.fixture
def mock_cache():
    with patch("app.server.get_flight") as mock_get, \
         patch("app.server.set_flight") as mock_set, \
         patch("app.server.invalidate_flight") as mock_inv_f, \
         patch("app.server.get_search") as mock_get_s, \
         patch("app.server.set_search") as mock_set_s, \
         patch("app.server.invalidate_search") as mock_inv_s:
        yield {
            "get_flight": mock_get,
            "set_flight": mock_set,
            "invalidate_flight": mock_inv_f,
            "get_search": mock_get_s,
            "set_search": mock_set_s,
            "invalidate_search": mock_inv_s,
        }


def test_get_flight_cache_hit(mock_db_session, mock_cache):
    cached_data = {
        "flight": {
            "id": "f1",
            "flight_number": "SU100",
            "origin": "SVO",
            "destination": "LED",
            "departure_time": "2026-05-10T08:00:00",
            "arrival_time": "2026-05-10T10:00:00",
            "total_seats": 100,
            "available_seats": 80,
            "price": 75.5,
            "status": FlightStatus.SCHEDULED.value,
        }
    }
    mock_cache["get_flight"].return_value = cached_data

    service = FlightService()
    request = flight_pb2.GetFlightRequest(id="f1")
    context = Mock()

    response = service.GetFlight(request, context)

    assert isinstance(response, flight_pb2.FlightResponse)
    assert response.flight.flight_number == "SU100"
    mock_db_session.get.assert_not_called()
    mock_cache["set_flight"].assert_not_called()


def test_get_flight_cache_miss(mock_db_session, mock_cache):
    mock_cache["get_flight"].return_value = None

    flight_db = Mock(spec=Flight)
    flight_db.id = "f2"
    flight_db.flight_number = "SU200"
    flight_db.origin = "SVO"
    flight_db.destination = "LED"
    flight_db.departure_time = datetime(2026, 5, 11, 8, 0)
    flight_db.arrival_time = datetime(2026, 5, 11, 10, 0)
    flight_db.total_seats = 150
    flight_db.available_seats = 150
    flight_db.price = 120.0
    flight_db.status = FlightStatus.SCHEDULED
    mock_db_session.get.return_value = flight_db

    service = FlightService()
    request = flight_pb2.GetFlightRequest(id="f2")
    context = Mock()

    response = service.GetFlight(request, context)

    assert response.flight.flight_number == "SU200"
    mock_db_session.get.assert_called_once_with(Flight, "f2")
    mock_cache["set_flight"].assert_called_once_with("f2", {"flight": ANY})


def test_get_flight_not_found(mock_db_session, mock_cache):
    mock_cache["get_flight"].return_value = None
    mock_db_session.get.return_value = None

    service = FlightService()
    request = flight_pb2.GetFlightRequest(id="invalid")
    context = Mock()
    context.abort = Mock()

    service.GetFlight(request, context)

    context.abort.assert_called_once_with(grpc.StatusCode.NOT_FOUND, "flight not found")


def test_search_flights_cache_hit(mock_db_session, mock_cache):
    cached = {
        "flights": [
            {
                "id": "f1",
                "flight_number": "SU100",
                "origin": "SVO",
                "destination": "LED",
                "departure_time": "2026-05-10T08:00:00",
                "arrival_time": "2026-05-10T10:00:00",
                "total_seats": 100,
                "available_seats": 80,
                "price": 75.5,
                "status": FlightStatus.SCHEDULED.value,
            }
        ]
    }
    mock_cache["get_search"].return_value = cached

    service = FlightService()
    request = flight_pb2.SearchFlightsRequest(origin="SVO", destination="LED")
    ts = Timestamp()
    ts.FromSeconds(1767916800)
    request.date.CopyFrom(ts)
    context = Mock()

    response = service.SearchFlights(request, context)

    assert len(response.flights) == 1
    assert response.flights[0].flight_number == "SU100"
    mock_db_session.query.assert_not_called()


def test_search_flights_cache_miss(mock_db_session, mock_cache):
    mock_cache["get_search"].return_value = None

    flight1 = Mock(spec=Flight)
    flight1.id = "f1"
    flight1.flight_number = "SU100"
    flight1.origin = "SVO"
    flight1.destination = "LED"
    flight1.departure_time = datetime(2026, 5, 10, 8, 0)
    flight1.arrival_time = datetime(2026, 5, 10, 10, 0)
    flight1.total_seats = 100
    flight1.available_seats = 80
    flight1.price = 75.5
    flight1.status = FlightStatus.SCHEDULED

    mock_db_session.query.return_value.filter.return_value.all.return_value = [flight1]

    service = FlightService()
    request = flight_pb2.SearchFlightsRequest(origin="SVO", destination="LED")
    ts = Timestamp()
    ts.FromSeconds(1767916800)
    request.date.CopyFrom(ts)
    context = Mock()

    response = service.SearchFlights(request, context)

    assert len(response.flights) == 1
    mock_cache["set_search"].assert_called_once()


def test_reserve_seats_success(mock_db_session, mock_cache):
    flight = Mock(spec=Flight)
    flight.id = "f1"
    flight.origin = "SVO"
    flight.destination = "LED"
    flight.departure_time = datetime(2026, 5, 10, 8, 0)
    flight.status = FlightStatus.SCHEDULED
    flight.available_seats = 10

    mock_db_session.query.return_value.with_for_update.return_value.get.return_value = flight
    mock_db_session.query.return_value.filter_by.return_value.first.return_value = None
    mock_db_session.add = Mock()
    mock_db_session.commit = Mock()

    service = FlightService()
    request = flight_pb2.ReserveSeatsRequest(
        flight_id="f1",
        booking_id="b1",
        seat_count=3
    )
    context = Mock()

    response = service.ReserveSeats(request, context)

    assert isinstance(response, flight_pb2.ReserveSeatsResponse)
    assert response.reservation_id is not None
    assert flight.available_seats == 7
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()
    mock_cache["invalidate_flight"].assert_called_once_with("f1")
    mock_cache["invalidate_search"].assert_called_once()


def test_reserve_seats_duplicate_booking(mock_db_session, mock_cache):
    existing = Mock(spec=SeatReservation)
    existing.id = "res_exist"
    mock_db_session.query.return_value.filter_by.return_value.first.return_value = existing

    service = FlightService()
    request = flight_pb2.ReserveSeatsRequest(
        flight_id="f1",
        booking_id="b1",
        seat_count=1
    )
    context = Mock()

    response = service.ReserveSeats(request, context)

    assert response.reservation_id == str(existing.id)
    mock_db_session.query.return_value.with_for_update.return_value.get.assert_not_called()
    mock_db_session.add.assert_not_called()
    mock_db_session.commit.assert_not_called()


def test_reserve_seats_not_enough(mock_db_session, mock_cache):
    flight = Mock()
    flight.available_seats = 2
    flight.status = FlightStatus.SCHEDULED
    mock_db_session.query.return_value.with_for_update.return_value.get.return_value = flight
    mock_db_session.query.return_value.filter_by.return_value.first.return_value = None

    service = FlightService()
    request = flight_pb2.ReserveSeatsRequest(
        flight_id="f1",
        booking_id="b1",
        seat_count=5
    )
    context = Mock()
    context.abort = Mock()

    service.ReserveSeats(request, context)

    context.abort.assert_called_once_with(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough seats")
    mock_db_session.commit.assert_not_called()


def test_reserve_seats_flight_not_found(mock_db_session, mock_cache):
    mock_db_session.query.return_value.filter_by.return_value.first.return_value = None

    mock_db_session.query.return_value.with_for_update.return_value.get.return_value = None

    service = FlightService()
    request = flight_pb2.ReserveSeatsRequest(
        flight_id="invalid",
        booking_id="b1",
        seat_count=1
    )
    context = Mock()
    context.abort = Mock()

    service.ReserveSeats(request, context)

    context.abort.assert_called_once_with(grpc.StatusCode.NOT_FOUND, "flight not found")


def test_release_reservation_success(mock_db_session, mock_cache):
    reservation = MagicMock(spec=SeatReservation)
    reservation.status = ReservationStatus.ACTIVE
    reservation.flight_id = "f1"
    reservation.seat_count = 2
    reservation.booking_id = "b1"

    flight = MagicMock(spec=Flight)
    flight.available_seats = 10
    flight.id = "f1"
    flight.origin = "SVO"
    flight.destination = "LED"
    flight.departure_time = datetime(2026, 5, 10, 8, 0)

    seat_res_query = Mock()
    seat_res_query.filter_by.return_value.with_for_update.return_value.first.return_value = reservation

    flight_query = Mock()
    flight_query.with_for_update.return_value.get.return_value = flight

    def query_side_effect(model):
        if model == SeatReservation:
            return seat_res_query
        elif model == Flight:
            return flight_query
        return Mock()

    mock_db_session.query.side_effect = query_side_effect
    mock_db_session.commit = Mock()

    service = FlightService()
    request = flight_pb2.ReleaseReservationRequest(booking_id="b1")
    context = Mock()

    response = service.ReleaseReservation(request, context)

    assert response.success is True
    assert reservation.status == ReservationStatus.RELEASED
    assert flight.available_seats == 12
    mock_db_session.commit.assert_called_once()
    mock_cache["invalidate_flight"].assert_called_once_with("f1")
    mock_cache["invalidate_search"].assert_called_once()


def test_release_reservation_not_found(mock_db_session, mock_cache):
    seat_res_query = Mock()

    seat_res_query.filter_by.return_value.with_for_update.return_value.first.return_value = None

    def query_side_effect(model):
        if model == SeatReservation:
            return seat_res_query
        return Mock()

    mock_db_session.query.side_effect = query_side_effect

    service = FlightService()
    request = flight_pb2.ReleaseReservationRequest(booking_id="nonexistent")
    context = Mock()
    context.abort = Mock()

    service.ReleaseReservation(request, context)

    context.abort.assert_called_once_with(grpc.StatusCode.NOT_FOUND, "reservation not found")
    mock_db_session.commit.assert_not_called()


def test_release_reservation_already_released(mock_db_session, mock_cache):
    reservation = Mock()
    reservation.status = ReservationStatus.RELEASED
    mock_db_session.query.return_value.filter.return_value.first.return_value = reservation

    service = FlightService()
    request = flight_pb2.ReleaseReservationRequest(booking_id="b1")
    context = Mock()

    response = service.ReleaseReservation(request, context)

    assert response.success is True
    mock_db_session.commit.assert_not_called()
    mock_cache["invalidate_flight"].assert_not_called()