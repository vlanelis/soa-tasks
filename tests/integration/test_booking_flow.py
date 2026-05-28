import pytest
import requests
import grpc
from uuid import uuid4
import os

import flight_pb2
import flight_pb2_grpc
from db_helper import insert_test_flight, delete_test_flight

BOOKING_URL = os.getenv("BOOKING_API_URL", "http://localhost:8000")
FLIGHT_GRPC_ADDR = os.getenv("FLIGHT_GRPC_ADDR", "localhost:50051")
API_KEY = os.getenv("TEST_API_KEY", "secret-service-key")

@pytest.fixture
def test_flight():
    flight_id = insert_test_flight()
    yield flight_id
    delete_test_flight(flight_id)

def test_search_flights_integration(test_flight):
    resp = requests.get(f"{BOOKING_URL}/flights", params={"origin": "SVO", "destination": "LED"})
    assert resp.status_code == 200
    flights = resp.json()
    assert any(f["id"] == test_flight for f in flights)

def test_create_booking_and_cancel(test_flight):
    flight_id = test_flight

    booking_data = {
        "user_id": str(uuid4()),
        "flight_id": flight_id,
        "passenger_name": "Integration Test",
        "passenger_email": "test@example.com",
        "seat_count": 1,
    }
    resp = requests.post(f"{BOOKING_URL}/bookings", json=booking_data)
    assert resp.status_code == 201
    booking = resp.json()
    booking_id = booking["id"]

    resp = requests.get(f"{BOOKING_URL}/bookings/{booking_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CONFIRMED"

    resp = requests.post(f"{BOOKING_URL}/bookings/{booking_id}/cancel")
    if resp.status_code != 200:
        print(f"Cancel failed: {resp.status_code} - {resp.text}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"

    channel = grpc.insecure_channel(FLIGHT_GRPC_ADDR)
    stub = flight_pb2_grpc.FlightServiceStub(channel)
    metadata = [("x-api-key", API_KEY)]
    req = flight_pb2.GetFlightRequest(id=flight_id)
    resp = stub.GetFlight(req, metadata=metadata)
    assert resp.flight.available_seats > 0