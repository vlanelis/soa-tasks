import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BOOKING_URL || 'http://booking-service:8000';

function randomUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export default function () {
  let searchRes = http.get(`${BASE_URL}/flights?origin=SVO&destination=LED`);
  check(searchRes, {
    'search status 200': (r) => r.status === 200,
  });
  if (searchRes.status !== 200) {
    sleep(1);
    return;
  }

  const flights = searchRes.json();
  if (!flights || flights.length === 0) {
    sleep(1);
    return;
  }
  const flightId = flights[0].id;

  const bookingPayload = JSON.stringify({
    user_id: randomUUID(),
    flight_id: flightId,
    passenger_name: `Load Test User ${__VU}`,
    passenger_email: `user${__VU}@example.com`,
    seat_count: 1,
  });
  const createRes = http.post(`${BASE_URL}/bookings`, bookingPayload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(createRes, {
    'create status 201': (r) => r.status === 201,
  });
  if (createRes.status !== 201) {
    sleep(1);
    return;
  }

  const booking = createRes.json();
  const bookingId = booking.id;

  const cancelRes = http.post(`${BASE_URL}/bookings/${bookingId}/cancel`);
  check(cancelRes, {
    'cancel status 200': (r) => r.status === 200,
  });

  sleep(0.5);
}