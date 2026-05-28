import pytest
from unittest.mock import Mock, patch
from uuid import uuid4
from decimal import Decimal
from datetime import datetime
from fastapi.testclient import TestClient
from grpc import RpcError, StatusCode

from app.main import app
from app.models import Booking, BookingStatus

client = TestClient(app)


class MockRpcError(RpcError):
    def __init__(self, code: StatusCode, details: str = ""):
        self._code = code
        self._details = details
        super().__init__(details)

    def code(self):
        return self._code

    def details(self):
        return self._details


@pytest.fixture
def mock_db_session():
    with patch("app.main.SessionLocal") as mock:
        session = Mock()
        mock.return_value = session
        yield session


@pytest.fixture
def mock_grpc_clients():
    with patch("app.main.get_flight") as mock_get, \
         patch("app.main.search_flights") as mock_search, \
         patch("app.main.reserve_seats") as mock_reserve, \
         patch("app.main.release_reservation") as mock_release:
        yield {
            "get": mock_get,
            "search": mock_search,
            "reserve": mock_reserve,
            "release": mock_release,
        }


def test_search_flights_success(mock_grpc_clients):
    mock_response = Mock()
    flight_mock = Mock()
    flight_mock.id = "f1"
    flight_mock.flight_number = "SU100"
    flight_mock.airline = "Aeroflot"
    flight_mock.origin = "SVO"
    flight_mock.destination = "LED"
    flight_mock.total_seats = 150
    flight_mock.available_seats = 120
    flight_mock.price = 75.5
    flight_mock.status = 0

    departure = datetime(2026, 5, 10, 8, 0)
    arrival = datetime(2026, 5, 10, 10, 0)
    flight_mock.departure_time.ToDatetime.return_value = departure
    flight_mock.arrival_time.ToDatetime.return_value = arrival
    mock_response.flights = [flight_mock]
    mock_grpc_clients["search"].return_value = mock_response

    response = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["flight_number"] == "SU100"
    assert "departure_time" in data[0]
    assert data[0]["departure_time"] == departure.isoformat()


def test_search_flights_with_date(mock_grpc_clients):
    mock_response = Mock()
    mock_response.flights = []
    mock_grpc_clients["search"].return_value = mock_response

    response = client.get("/flights", params={"origin": "SVO", "destination": "LED", "date": "2026-05-10"})
    assert response.status_code == 200

    call_args = mock_grpc_clients["search"].call_args
    assert call_args[0][0] == "SVO"
    assert call_args[0][1] == "LED"
    date_ts = call_args[0][2]
    assert date_ts is not None

    assert isinstance(date_ts, int)


def test_search_flights_grpc_error(mock_grpc_clients):
    mock_grpc_clients["search"].side_effect = Exception("Connection refused")
    response = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
    assert response.status_code == 503
    assert response.json()["detail"] == "flight service unavailable"


def test_get_flight_success(mock_grpc_clients):
    flight_resp = Mock()
    flight = Mock()
    flight_id = str(uuid4())
    flight.id = flight_id
    flight.flight_number = "SU100"
    flight.airline = "Aeroflot"
    flight.origin = "SVO"
    flight.destination = "LED"
    flight.total_seats = 150
    flight.available_seats = 120
    flight.price = 75.5
    flight.status = 0
    flight_resp.flight = flight
    mock_grpc_clients["get"].return_value = flight_resp

    response = client.get(f"/flights/{flight_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == flight_id
    assert data["flight_number"] == "SU100"


def test_get_flight_not_found(mock_grpc_clients):
    err = MockRpcError(StatusCode.NOT_FOUND, "flight not found")
    mock_grpc_clients["get"].side_effect = err
    response = client.get(f"/flights/{str(uuid4())}")
    assert response.status_code == 404
    assert response.json()["detail"] == "flight not found"


def test_create_booking_success(mock_grpc_clients, mock_db_session):
    flight_resp = Mock()
    flight_resp.flight.price = 100.0
    mock_grpc_clients["get"].return_value = flight_resp

    reserve_resp = Mock()
    reserve_resp.reservation_id = "res-123"
    mock_grpc_clients["reserve"].return_value = reserve_resp

    db_session = mock_db_session
    db_session.add = Mock()
    db_session.commit = Mock()
    db_session.close = Mock()

    booking_data = {
        "user_id": str(uuid4()),
        "flight_id": str(uuid4()),
        "passenger_name": "John Doe",
        "passenger_email": "john@example.com",
        "seat_count": 2,
    }
    response = client.post("/bookings", json=booking_data)
    assert response.status_code == 201
    json_resp = response.json()
    assert "id" in json_resp
    assert json_resp["total_price"] == 200.0
    assert json_resp["reservation_id"] == "res-123"

    db_session.add.assert_called_once()
    added_booking = db_session.add.call_args[0][0]
    assert added_booking.seat_count == 2
    assert added_booking.total_price == Decimal("200.00")
    assert added_booking.status == BookingStatus.CONFIRMED


def test_create_booking_flight_not_found(mock_grpc_clients, mock_db_session):
    err = MockRpcError(StatusCode.NOT_FOUND, "flight not found")
    mock_grpc_clients["get"].side_effect = err
    booking_data = {
        "user_id": str(uuid4()),
        "flight_id": str(uuid4()),
        "passenger_name": "John",
        "passenger_email": "john@example.com",
        "seat_count": 1,
    }
    response = client.post("/bookings", json=booking_data)
    assert response.status_code == 404
    mock_db_session.add.assert_not_called()


def test_create_booking_not_enough_seats(mock_grpc_clients, mock_db_session):
    flight_resp = Mock()
    flight_resp.flight.price = 100.0
    mock_grpc_clients["get"].return_value = flight_resp
    err = MockRpcError(StatusCode.RESOURCE_EXHAUSTED, "not enough seats")
    mock_grpc_clients["reserve"].side_effect = err

    booking_data = {
        "user_id": str(uuid4()),
        "flight_id": str(uuid4()),
        "passenger_name": "John",
        "passenger_email": "john@example.com",
        "seat_count": 100,
    }
    response = client.post("/bookings", json=booking_data)
    assert response.status_code == 409
    assert response.json()["detail"] == "not enough seats"
    mock_db_session.add.assert_not_called()


def test_cancel_booking_success(mock_grpc_clients, mock_db_session):
    booking_id = uuid4()
    mock_booking = Mock(spec=Booking)
    mock_booking.id = booking_id
    mock_booking.status = BookingStatus.CONFIRMED
    db_session = mock_db_session
    db_session.get.return_value = mock_booking
    db_session.commit = Mock()
    mock_grpc_clients["release"].return_value = Mock()

    response = client.post(f"/bookings/{booking_id}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "CANCELLED"
    assert mock_booking.status == BookingStatus.CANCELLED
    mock_db_session.add.assert_called_once_with(mock_booking)
    mock_db_session.commit.assert_called_once()
    mock_grpc_clients["release"].assert_called_once_with(str(booking_id))


def test_cancel_booking_already_cancelled(mock_grpc_clients, mock_db_session):
    booking_id = uuid4()
    mock_booking = Mock(spec=Booking)
    mock_booking.status = BookingStatus.CANCELLED
    mock_db_session.get.return_value = mock_booking

    response = client.post(f"/bookings/{booking_id}/cancel")
    assert response.status_code == 400
    assert response.json()["detail"] == "booking is not confirmed"
    mock_grpc_clients["release"].assert_not_called()


def test_get_booking_success(mock_db_session):
    booking_id = uuid4()
    mock_booking = Mock()
    mock_booking.id = booking_id
    mock_booking.user_id = uuid4()
    mock_booking.flight_id = uuid4()
    mock_booking.passenger_name = "John"
    mock_booking.passenger_email = "john@ex.com"
    mock_booking.seat_count = 2
    mock_booking.total_price = Decimal("150.00")
    mock_booking.status = BookingStatus.CONFIRMED
    mock_db_session.get.return_value = mock_booking

    response = client.get(f"/bookings/{booking_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(booking_id)
    assert data["status"] == "CONFIRMED"


def test_list_bookings(mock_db_session):
    bookings = [
        Booking(id=1, flight_id="FL123", seat_count=12, user_id="user1", total_price=100, status=BookingStatus.CONFIRMED),
        Booking(id=2, flight_id="FL456", seat_count=5, user_id="user1", total_price=20, status=BookingStatus.CONFIRMED),
    ]

    mock_db_session.query.return_value.filter.return_value.all.return_value = bookings
    response = client.get("/bookings?user_id=user1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["flight_id"] == "FL123"
    assert data[1]["seat_count"] == 5