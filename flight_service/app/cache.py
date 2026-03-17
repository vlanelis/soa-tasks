import json
import datetime
from redis.sentinel import Sentinel

from .config import REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT, REDIS_MASTER_NAME

sentinel = Sentinel([(REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT)], socket_timeout=0.1)
redis_master = sentinel.master_for(REDIS_MASTER_NAME, socket_timeout=0.1, decode_responses=True)


TTL_FLIGHT = 600
TTL_SEARCH = 300


def _serialize(v):
    return json.dumps(v)


def _deserialize(v):
    return json.loads(v) if v else None


# ---------- keys ----------

def search_key(origin, destination, date=None):
    if date:
        return f"search:{origin}:{destination}:{date}"
    return f"search:{origin}:{destination}:ALL"

def flight_key(fid: str):
    return f"flight:{fid}"

# ---------- cache ----------


def get_flight(fid: str):
    return _deserialize(redis_master.get(flight_key(fid)))


def set_flight(fid: str, data: dict):
    redis_master.setex(flight_key(fid), TTL_FLIGHT, _serialize(data))


def invalidate_flight(fid: str):
    redis_master.delete(flight_key(fid))


def get_search(origin, destination, date):
    return _deserialize(redis_master.get(
        search_key(origin, destination, date)
    ))


def set_search(origin, destination, date, data):
    redis_master.setex(
        search_key(origin, destination, date),
        TTL_SEARCH,
        _serialize(data)
    )


def invalidate_search(origin, destination, date):
    keys = [
        search_key(origin, destination, date),
        search_key(origin, destination, None)
    ]

    redis_master.delete(*keys)
