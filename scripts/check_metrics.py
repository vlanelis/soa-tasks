#!/usr/bin/env python3
import requests
import sys
import time


PROMETHEUS_URL = "http://localhost:9090"
TIMEFRAME = "2m"

THRESHOLDS = {
    "booking_error_rate": 0.01,
    "flight_error_rate": 0.01,
    "booking_p95_latency": 0.5,
    "flight_p95_latency": 0.5,
    "booking_availability": 0.95,
}

def query_prometheus(query):
    resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
    data = resp.json()
    if data["status"] != "success" or not data["data"]["result"]:
        return None
    try:
        return float(data["data"]["result"][0]["value"][1])
    except (IndexError, KeyError, ValueError):
        return None

def check_booking_error_rate():
    query = f"""
    (
      sum without(instance, job, method, endpoint, status) (
        rate(http_requests_total{{job="booking-service",status=~"5.."}}[{TIMEFRAME}])
      ) or vector(0)
    )
    /
    (
      sum without(instance, job, method, endpoint, status) (
        rate(http_requests_total{{job="booking-service"}}[{TIMEFRAME}])
      ) or vector(0)
    )
    """
    val = query_prometheus(query) or 0.0
    print(f"Booking error rate: {val:.4f}")
    return val <= THRESHOLDS["booking_error_rate"]

def check_flight_error_rate():
    query = f"""
    (
      sum without(instance, job, method, error_type) (
        rate(grpc_errors_total{{job="flight-service",error_type=~"INTERNAL|UNAVAILABLE|UNKNOWN|DATA_LOSS|ABORTED"}}[{TIMEFRAME}])
      ) or vector(0)
    )
    /
    (
      sum without(instance, job, method, status) (
        rate(grpc_requests_total{{job="flight-service"}}[{TIMEFRAME}])
      ) or vector(0)
    )
    """
    val = query_prometheus(query) or 0.0
    print(f"Flight error rate: {val:.4f}")
    return val <= THRESHOLDS["flight_error_rate"]

def check_booking_p95_latency():
    query = f"""
    histogram_quantile(0.95,
      sum(rate(http_request_duration_seconds_bucket{{job="booking-service"}}[{TIMEFRAME}])) by (le)
    )
    """
    val = query_prometheus(query) or 0.0
    print(f"Booking p95 latency: {val*1000:.2f} ms")
    return val <= THRESHOLDS["booking_p95_latency"]

def check_flight_p95_latency():
    query = f"""
    histogram_quantile(0.95,
      sum(rate(grpc_request_duration_seconds_bucket{{job="flight-service"}}[{TIMEFRAME}])) by (le)
    )
    """
    val = query_prometheus(query) or 0.0
    print(f"Flight p95 latency: {val*1000:.2f} ms")
    return val <= THRESHOLDS["flight_p95_latency"]

def check_booking_availability():
    query = f"""
    sum(rate(http_requests_total{{job="booking-service",status!~"5.."}}[{TIMEFRAME}]))
    /
    sum(rate(http_requests_total{{job="booking-service"}}[{TIMEFRAME}]))
    """
    val = query_prometheus(query) or 1.0
    print(f"Booking availability: {val*100:.2f}%")
    return val >= THRESHOLDS["booking_availability"]

def main():
    time.sleep(5)
    checks = [
        ("Booking error rate", check_booking_error_rate),
        ("Flight error rate", check_flight_error_rate),
        ("Booking p95 latency", check_booking_p95_latency),
        ("Flight p95 latency", check_flight_p95_latency),
        ("Booking availability", check_booking_availability),
    ]
    ok = True
    for name, check_func in checks:
        if not check_func():
            print(f"FAILED: {name}")
            ok = False
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
