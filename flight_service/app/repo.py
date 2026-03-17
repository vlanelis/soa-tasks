from sqlalchemy import select
from .models import Flight, SeatReservation, ReservationStatus, FlightStatus


def get_flight_by_id(db, fid):
    return db.get(Flight, fid)


def search_flights(db, origin, destination, date=None):
    q = select(Flight).where(Flight.origin == origin, Flight.destination == destination, Flight.status == FlightStatus.SCHEDULED)
    return db.execute(q).scalars().all()


def reserve_seats_tx(db, flight_id, booking_id, seat_count):
    flight = db.get(Flight, flight_id, with_for_update=True)
    if flight is None:
        raise LookupError("not found")
    if flight.available_seats < seat_count:
        raise ValueError("not enough seats")
    existing = db.query(SeatReservation).filter_by(booking_id=booking_id).first()
    if existing:
        return existing
    flight.available_seats -= seat_count
    res = SeatReservation(flight_id=flight_id, booking_id=booking_id, seat_count=seat_count, status=ReservationStatus.ACTIVE)
    db.add(res)
    return res


def release_reservation_tx(db, booking_id):
    res = db.query(SeatReservation).filter_by(booking_id=booking_id, status=ReservationStatus.ACTIVE).first()
    if not res:
        return False
    flight = db.get(Flight, res.flight_id, with_for_update=True)
    if flight is None:
        return False
    flight.available_seats += res.seat_count
    res.status = ReservationStatus.RELEASED
    return True
