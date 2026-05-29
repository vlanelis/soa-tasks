from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field, EmailStr
from typing import Optional
from uuid import UUID, uuid4
from decimal import Decimal
import logging

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy.exc import IntegrityError

from .database import SessionLocal
from .middleware import PrometheusMiddleware
from .models import Booking, BookingStatus
from .grpc_client import get_flight, reserve_seats, release_reservation, search_flights


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("booking_service")
app = FastAPI(title="Booking Service")
app.add_middleware(PrometheusMiddleware)


class CreateBookingReq(BaseModel):
    user_id: UUID
    flight_id: UUID
    passenger_name: str
    passenger_email: EmailStr
    seat_count: int = Field(gt=0)


class BookingResp(BaseModel):
    id: UUID
    user_id: UUID
    flight_id: UUID
    passenger_name: str
    passenger_email: EmailStr
    seat_count: int
    total_price: Decimal
    status: str


@app.get("/flights")
def flights(origin: str = Query(...), destination: str = Query(...), date: Optional[str] = None):
    try:
        date_ts = None
        if date:
            import datetime
            d = datetime.datetime.fromisoformat(date)
            date_ts = int(d.timestamp())
        resp = search_flights(origin, destination, date_ts)
        out = []
        for f in resp.flights:
            data = {
                "id": f.id,
                "flight_number": f.flight_number,
                "airline": f.airline,
                "origin": f.origin,
                "destination": f.destination,
                "total_seats": f.total_seats,
                "available_seats": f.available_seats,
                "price": float(f.price),
                "status": f.status
            }
            departure_time = f.departure_time.ToDatetime()
            if departure_time.timestamp() != 0:
                data["departure_time"] = departure_time.isoformat()
            arrival_time = f.arrival_time.ToDatetime()
            if arrival_time.timestamp() != 0:
                data["arrival_time"] = arrival_time.isoformat()
            out.append(data)
        return out
    except Exception as e:
        logger.exception("search flights fail")
        raise HTTPException(status_code=503, detail="flight service unavailable")


@app.get("/flights/{id}")
def get_flight_by_id(id: UUID):
    try:
        resp = get_flight(str(id))
        f = resp.flight
        return {
            "id": f.id,
            "flight_number": f.flight_number,
            "airline": f.airline,
            "origin": f.origin,
            "destination": f.destination,
            "total_seats": f.total_seats,
            "available_seats": f.available_seats,
            "price": float(f.price),
            "status": f.status
        }
    except Exception as e:
        import grpc
        if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="flight not found")
        logger.exception("get flight error")
        raise HTTPException(status_code=503, detail="flight service unavailable")


@app.post("/bookings", status_code=201)
def create_booking(req: CreateBookingReq):
    db = SessionLocal()
    try:
        flight_resp = get_flight(str(req.flight_id))
        price = float(flight_resp.flight.price)
    except Exception as e:
        import grpc
        if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail="flight not found")
        logger.exception("get_flight failed")
        raise HTTPException(status_code=503, detail="flight service unavailable")

    booking_id = str(uuid4())

    try:
        reserve_resp = reserve_seats(str(req.flight_id), booking_id, req.seat_count)
        reservation_id = reserve_resp.reservation_id
    except Exception as e:
        import grpc
        if isinstance(e, grpc.RpcError) and e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
            raise HTTPException(status_code=409, detail="not enough seats")
        logger.exception("reserve_seats failed")
        raise HTTPException(status_code=503, detail="flight service unavailable")

    from decimal import Decimal
    total_price = Decimal(req.seat_count) * Decimal(str(price))
    booking = Booking(
        id=booking_id,
        user_id=req.user_id,
        flight_id=req.flight_id,
        passenger_name=req.passenger_name,
        passenger_email=req.passenger_email,
        seat_count=req.seat_count,
        total_price=total_price,
        status=BookingStatus.CONFIRMED
    )
    try:
        db.add(booking)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=500, detail="db error")
    finally:
        db.close()

    return {"id": booking_id, "total_price": float(total_price), "reservation_id": reservation_id}


@app.get("/bookings/{id}")
def get_booking(id: UUID):
    db = SessionLocal()
    try:
        b = db.get(Booking, id)
        if not b:
            raise HTTPException(status_code=404, detail="booking not found")
        return {
            "id": str(b.id),
            "user_id": str(b.user_id),
            "flight_id": str(b.flight_id),
            "passenger_name": b.passenger_name,
            "passenger_email": b.passenger_email,
            "seat_count": b.seat_count,
            "total_price": float(b.total_price),
            "status": b.status.value
        }
    finally:
        db.close()


@app.post("/bookings/{id}/cancel")
def cancel_booking(id: UUID):
    db = SessionLocal()
    try:
        b = db.get(Booking, id)
        if not b:
            raise HTTPException(status_code=404, detail="booking not found")
        if b.status != BookingStatus.CONFIRMED:
            raise HTTPException(status_code=400, detail="booking is not confirmed")
        # call FlightService ReleaseReservation
        try:
            resp = release_reservation(str(b.id))
        except Exception:
            logger.exception("release reservation failed")
            raise HTTPException(status_code=503, detail="flight service unavailable")
        # update booking
        b.status = BookingStatus.CANCELLED
        db.add(b)
        db.commit()
        return {"id": str(b.id), "status": "CANCELLED"}
    finally:
        db.close()


@app.get("/bookings")
def list_bookings(user_id: Optional[str] = None):
    db = SessionLocal()
    try:
        q = db.query(Booking)
        if user_id:
            q = q.filter(Booking.user_id == user_id)
        rows = q.all()
        out = []
        for b in rows:
            out.append({
                "id": str(b.id),
                "user_id": str(b.user_id),
                "flight_id": str(b.flight_id),
                "passenger_name": b.passenger_name,
                "seat_count": b.seat_count,
                "total_price": float(b.total_price),
                "status": b.status.value
            })
        return out
    finally:
        db.close()


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok"}
