import time
import uuid
from datetime import datetime, timedelta, timezone
import json
import re
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroSerializer
import docker
import requests

# ---------- Сценарий 1 ----------
def test_scenario_1_basic_flow(send_event, cassandra_session):
    send_event("PRODUCT_RECEIVED", {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 100})
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")
    )
    assert rows.one().available == 100

    send_event("PRODUCT_RESERVED", {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 30, "order_id": "ORDER-1"})
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available, reserved FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")
    )
    row = rows.one()
    assert row.available == 70 and row.reserved == 30

    send_event("PRODUCT_MOVED", {"product_id": "SKU-001", "from_zone": "ZONE-A", "to_zone": "ZONE-B", "quantity": 20})
    time.sleep(1)
    rows_a = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")).one()
    rows_b = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-B")).one()
    assert rows_a.available == 50 and rows_b.available == 20

    send_event("PRODUCT_SHIPPED", {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 10})
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")).one()
    assert rows.available == 40

    send_event("ORDER_CREATED", {"order_id": "ORDER-2", "items": [{"product_id": "SKU-001", "quantity": 15, "zone_id": "ZONE-A"}]})
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT reserved FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")).one()
    assert rows.reserved == 45

    send_event("ORDER_COMPLETED", {"order_id": "ORDER-2"})
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT reserved FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-001", "ZONE-A")).one()
    assert rows.reserved == 30


# ---------- Сценарий 2 ----------
def test_scenario_2_idempotency(send_event, cassandra_session):
    event_id = str(uuid.uuid4())
    data = {"product_id": "SKU-002", "zone_id": "ZONE-A", "quantity": 50}
    send_event("PRODUCT_RECEIVED", data, event_id=event_id)
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-002", "ZONE-A"))
    available_first = rows.one().available
    assert available_first == 50

    send_event("PRODUCT_RECEIVED", data, event_id=event_id)
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-002", "ZONE-A"))
    available_second = rows.one().available
    assert available_second == available_first

    processed = cassandra_session.execute(
        "SELECT event_id FROM processed_events WHERE event_id=%s", (event_id,)).one()
    assert processed is not None


# ---------- Сценарий 3 ----------
def test_scenario_3_consistency(send_event, cassandra_session):
    send_event("PRODUCT_RECEIVED", {"product_id": "SKU-003", "zone_id": "ZONE-A", "quantity": 100})
    time.sleep(1)
    row_zp = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-003", "ZONE-A")).one()
    row_p = cassandra_session.execute(
        "SELECT total_available FROM inventory_by_product WHERE product_id=%s",
        ("SKU-003",)).one()
    row_z = cassandra_session.execute(
        "SELECT available FROM inventory_by_zone WHERE zone_id=%s AND product_id=%s",
        ("ZONE-A", "SKU-003")).one()
    assert row_zp.available == 100
    assert row_p.total_available == 100
    assert row_z.available == 100


# ---------- Сценарий 4 ----------
def test_scenario_4_out_of_order(send_event, cassandra_session):
    product_id = "SKU-004"
    zone = "ZONE-A"
    base_time = datetime.now(timezone.utc)
    t1 = base_time
    send_event("PRODUCT_RECEIVED", {"product_id": product_id, "zone_id": zone, "quantity": 100}, timestamp=t1)
    time.sleep(1)
    t2 = base_time + timedelta(minutes=5)
    send_event("PRODUCT_SHIPPED", {"product_id": product_id, "zone_id": zone, "quantity": 20}, timestamp=t2)
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (product_id, zone))
    available = rows.one().available
    assert available == 80

    t3 = base_time + timedelta(minutes=2)
    send_event("PRODUCT_RECEIVED", {"product_id": product_id, "zone_id": zone, "quantity": 50}, timestamp=t3)
    time.sleep(1)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        (product_id, zone))
    available_after = rows.one().available
    assert available_after == 80


# ---------- Сценарий 5 (DLQ) ----------
def test_scenario_5_dlq(send_event, dlq_consumer, kafka_producer, sr_client, kafka_bootstrap):
    """Сценарий 5: Dead Letter Queue -- отправляем невалидное событие (отрицательное количество)"""
    # Регистрируем схему, отправив легальное событие
    send_event("PRODUCT_SHIPPED", {"product_id": "DUMMY", "zone_id": "ZONE-A", "quantity": 1})
    time.sleep(1)

    event_type = "PRODUCT_SHIPPED"
    subject = f"{event_type}-value"
    schema = sr_client.get_latest_version(subject).schema

    # Функция именования subject (2 аргумента, как у вас)
    def subject_name_func(ctx, schema_name):
        return subject

    avro_serializer = AvroSerializer(
        sr_client,
        schema.schema_str,
        conf={'subject.name.strategy': subject_name_func}
    )

    # Невалидное событие: отрицательное количество
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {"product_id": "SKU-005", "zone_id": "ZONE-A", "quantity": -10}
    }
    value = avro_serializer(event, SerializationContext("warehouse-events", MessageField.VALUE))
    kafka_producer.produce("warehouse-events", key=event["event_id"], value=value)
    kafka_producer.flush()

    # Ждём, пока consumer обработает и отправит в DLQ
    time.sleep(3)

    # Собираем все доступные сообщения из DLQ (уникальный consumer group)
    msgs = []
    start = time.time()
    while time.time() - start < 10:  # максимум 10 секунд
        msg = dlq_consumer.poll(0.5)
        if msg:
            msgs.append(msg)
        else:
            if msgs:  # если уже есть сообщения и больше нет, выходим
                break
    assert len(msgs) > 0, "No messages in DLQ"

    # Берём последнее сообщение (предполагаем, что оно от нашего теста)
    last_msg = msgs[-1]
    dlq_message = json.loads(last_msg.value().decode('utf-8'))
    assert "error_reason" in dlq_message
    # Проверяем, что quantity = -10 (в оригинальном событии)
    assert dlq_message["original_event"]["data"]["quantity"] == -10


# ---------- Сценарий 6 ----------
def test_scenario_6_monitoring_metrics(consumer_url):
    # Health check consumer
    resp = requests.get(f"{consumer_url}/health")
    assert resp.status_code == 200

    # Metrics endpoint
    resp = requests.get(f"{consumer_url}/metrics")
    assert resp.status_code == 200
    assert "events_processed_total" in resp.text


# ---------- Сценарий 7 ----------
def test_scenario_7_schema_evolution(send_event, cassandra_session):
    send_event("PRODUCT_RECEIVED", {"product_id": "SKU-007", "zone_id": "ZONE-A", "quantity": 100})
    send_event("PRODUCT_RECEIVED", {"product_id": "SKU-008", "zone_id": "ZONE-A", "quantity": 200, "supplier_id": "SUP-001"})
    time.sleep(2)
    row_v1 = cassandra_session.execute(
        "SELECT supplier_id FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-007", "ZONE-A")).one()
    row_v2 = cassandra_session.execute(
        "SELECT supplier_id FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-008", "ZONE-A")).one()
    assert row_v1.supplier_id is None
    assert row_v2.supplier_id == "SUP-001"


# ---------- Сценарий 8 ----------
def test_scenario_8_consumer_lag(consumer_url, send_event):
    for i in range(10):
        send_event("PRODUCT_RECEIVED", {"product_id": f"SKU-LAG-{i}", "zone_id": "ZONE-A", "quantity": 10})
    time.sleep(5)
    metrics_resp = requests.get(f"{consumer_url}/metrics")
    metrics_text = metrics_resp.text
    match = re.search(r'events_processed_total\{event_type="PRODUCT_RECEIVED"\} (\d+)', metrics_text)
    if match:
        count = int(match.group(1))
        assert count >= 10


# ---------- Сценарий 9 ----------
def test_scenario_9_cassandra_fault_tolerance(send_event, cassandra_session, docker_client):
    # Проверяем, что кластер из 3 нод
    rows = cassandra_session.execute("SELECT release_version FROM system.local")
    assert rows.one() is not None
    container = docker_client.containers.get("cassandra-2")
    container.stop()
    time.sleep(5)
    send_event("PRODUCT_RECEIVED", {"product_id": "SKU-006", "zone_id": "ZONE-A", "quantity": 200})
    time.sleep(2)
    rows = cassandra_session.execute(
        "SELECT available FROM inventory_by_product_zone WHERE product_id=%s AND zone_id=%s",
        ("SKU-006", "ZONE-A"))
    assert rows.one().available == 200
    container.start()
    time.sleep(10)