import pytest
import requests
import psycopg2
from uuid import uuid4
import os

BOOKING_URL = os.getenv("BOOKING_API_URL", "http://booking-service:8000")
BOOKING_DB_DSN = os.getenv("BOOKING_DB_DSN", "postgresql://postgres:postgres@booking-db:5432/booking")
FLIGHT_DB_DSN = os.getenv("FLIGHT_DB_DSN", "postgresql://postgres:postgres@flight-db:5432/flight")

def test_full_booking_lifecycle():
    conn_flight = psycopg2.connect(FLIGHT_DB_DSN)
    cur = conn_flight.cursor()
    flight_id = str(uuid4())
    cur.execute("""
        INSERT INTO flights (id, flight_number, airline, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (flight_id, "E2E100", "E2E Air", "SVO", "LED", "2026-06-01 08:00:00", "2026-06-01 10:00:00", 100, 100, 150.0, "SCHEDULED"))
    conn_flight.commit()
    cur.close()

    try:
        resp = requests.get(f"{BOOKING_URL}/flights", params={"origin": "SVO", "destination": "LED"})
        assert resp.status_code == 200
        flights = resp.json()
        assert any(f["id"] == flight_id for f in flights)
        original_available = next(f["available_seats"] for f in flights if f["id"] == flight_id)

        booking_data = {
            "user_id": str(uuid4()),
            "flight_id": flight_id,
            "passenger_name": "E2E User",
            "passenger_email": "e2e@example.com",
            "seat_count": 2,
        }
        resp = requests.post(f"{BOOKING_URL}/bookings", json=booking_data)
        assert resp.status_code == 201
        booking_id = resp.json()["id"]

        conn_booking = psycopg2.connect(BOOKING_DB_DSN)
        cur = conn_booking.cursor()
        cur.execute("SELECT status, seat_count, total_price FROM bookings WHERE id = %s", (booking_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "CONFIRMED"
        assert row[1] == 2
        assert float(row[2]) == 300.0  # 150 * 2
        conn_booking.close()

        cur = conn_flight.cursor()
        cur.execute("SELECT available_seats FROM flights WHERE id = %s", (flight_id,))
        new_available = cur.fetchone()[0]
        assert new_available == original_available - 2

        resp = requests.post(f"{BOOKING_URL}/bookings/{booking_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "CANCELLED"

        conn_booking = psycopg2.connect(BOOKING_DB_DSN)
        cur = conn_booking.cursor()
        cur.execute("SELECT status FROM bookings WHERE id = %s", (booking_id,))
        assert cur.fetchone()[0] == "CANCELLED"
        conn_booking.close()

        cur = conn_flight.cursor()
        cur.execute("SELECT available_seats FROM flights WHERE id = %s", (flight_id,))
        restored_available = cur.fetchone()[0]
        assert restored_available == original_available

    finally:
        cur = conn_flight.cursor()
        cur.execute("DELETE FROM seat_reservations WHERE flight_id = %s", (flight_id,))
        cur.execute("DELETE FROM flights WHERE id = %s", (flight_id,))
        conn_flight.commit()
        conn_flight.close()
