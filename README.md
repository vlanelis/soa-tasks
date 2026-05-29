### ER-диаграмма

```mermaid
erDiagram
    FLIGHTS {
        uuid id PK
        string flight_number "уникально в комбинации с departure_time"
        string airline
        string origin
        string destination
        datetime departure_time
        datetime arrival_time
        int total_seats "> 0"
        int available_seats "≥ 0"
        decimal price "> 0"
        enum status "SCHEDULED, DEPARTED, CANCELLED, COMPLETED"
    }

    SEAT_RESERVATIONS {
        uuid id PK
        uuid flight_id FK "ссылка на FLIGHTS.id"
        uuid booking_id "уникален, ссылка на Booking из другого сервиса"
        int seat_count "> 0"
        enum status "ACTIVE, RELEASED, EXPIRED"
    }

    BOOKINGS {
        uuid id PK
        uuid user_id "идентификатор пользователя (внешний сервис)"
        uuid flight_id "ссылка на FLIGHTS.id (логическая)"
        string passenger_name
        string passenger_email
        int seat_count "> 0"
        decimal total_price "≥ 0"
        enum status "CONFIRMED, CANCELLED"
        datetime created_at
    }

    FLIGHTS ||--o{ SEAT_RESERVATIONS : "имеет"
    FLIGHTS ||--o{ BOOKINGS : "бронируется в"
    BOOKINGS ||--|| SEAT_RESERVATIONS : "соответствует (один к одному, логически)"
```


# SLI (Service Level Indicators)

## 1. Доступность API (Booking Service)
- **Что измеряется:** доля успешных запросов (не 5xx) за последние 5 минут.
- **PromQL:**
  ```promql
  sum(rate(http_requests_total{job="booking-service",status!~"5.."}[5m]))
  /
  sum(rate(http_requests_total{job="booking-service"}[5m]))
  ```
- **SLO:** > 99.5%
- **Порог отказа:** < 95%
- **Использование:** проверка в CI после нагрузочного теста, алерт `HighErrorRate_Booking`.

## 2. Задержка p95 (Booking Service)
- **Что измеряется:** 95-й перцентиль времени ответа на запросы за последние 5 минут.
- **PromQL:**
  ```promql
  histogram_quantile(0.95,
    sum(rate(http_request_duration_seconds_bucket{job="booking-service"}[5m])) by (le)
  )
  ```
- **SLO:** < 500 мс
- **Порог отказа:** > 1000 мс
- **Использование:** проверка в CI, алерт `HighLatency_Booking`.

## 3. Доля серверных ошибок (Flight Service)
- **Что измеряется:** доля gRPC-вызовов с кодами INTERNAL, UNAVAILABLE, UNKNOWN, DATA_LOSS, ABORTED за последние 5 минут.
- **PromQL:**
  ```promql
  sum(rate(grpc_errors_total{job="flight-service",error_type=~"INTERNAL|UNAVAILABLE|UNKNOWN|DATA_LOSS|ABORTED"}[5m]))
  /
  sum(rate(grpc_requests_total{job="flight-service"}[5m]))
  ```
- **SLO:** < 1%
- **Порог отказа:** > 5%
- **Использование:** проверка в CI, алерт `HighErrorRate_Flight`.
