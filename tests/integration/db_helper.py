import psycopg2
import os
from uuid import uuid4
from datetime import datetime

FLIGHT_DB_DSN = os.getenv("FLIGHT_DB_DSN", "postgresql://postgres:postgres@flight-db:5432/flight")

def insert_test_flight(origin="SVO", destination="LED", departure_time=None, total_seats=100, available_seats=100, price=100.0):
    conn = psycopg2.connect(FLIGHT_DB_DSN)
    cur = conn.cursor()
    flight_id = str(uuid4())
    if departure_time is None:
        departure_time = datetime(2026, 5, 10, 8, 0)
    arrival_time = departure_time.replace(hour=10, minute=0)
    cur.execute("""
        INSERT INTO flights (id, flight_number, airline, origin, destination, departure_time, arrival_time, total_seats, available_seats, price, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        flight_id, "TEST100", "TestAirline", origin, destination,
        departure_time, arrival_time, total_seats, available_seats, price, "SCHEDULED"
    ))
    conn.commit()
    cur.close()
    conn.close()
    return flight_id

def delete_test_flight(flight_id):
    conn = psycopg2.connect(FLIGHT_DB_DSN)
    cur = conn.cursor()
    # Удаляем связанные резервации (если есть)
    cur.execute("DELETE FROM seat_reservations WHERE flight_id = %s", (flight_id,))
    cur.execute("DELETE FROM flights WHERE id = %s", (flight_id,))
    conn.commit()
    cur.close()
    conn.close()