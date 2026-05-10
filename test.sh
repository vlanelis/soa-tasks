#!/bin/bash

PRODUCER_URL=${PRODUCER_URL:-"http://localhost:8000"}
CASSANDRA_HOST=${CASSANDRA_HOST:-"localhost"}

echo "=== Сценарий 1: Базовый цикл склада ==="
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 100}
}'
sleep 1
echo -e "\nПроверка остатка в ZONE-A:"
cqlsh $CASSANDRA_HOST -e "SELECT available FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RESERVED",
  "data": {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 30, "order_id": "ORDER-1"}
}'
sleep 1
echo "Резерв 30:"
cqlsh $CASSANDRA_HOST -e "SELECT available, reserved FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_MOVED",
  "data": {"product_id": "SKU-001", "from_zone": "ZONE-A", "to_zone": "ZONE-B", "quantity": 20}
}'
sleep 1
echo "Перемещение 20 из A в B:"
cqlsh $CASSANDRA_HOST -e "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001';"

curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_SHIPPED",
  "data": {"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 10}
}'
sleep 1
echo "Отгрузка 10 из A:"
cqlsh $CASSANDRA_HOST -e "SELECT available FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

echo -e "\n=== Сценарий 2: Идемпотентность ==="
echo "Отправляем одно и то же событие дважды (одинаковый event_id) – в реальном producer это не реализовано, поэтому тест демонстрирует только механизм."
echo "Ручная проверка: после дубля остаток не должен удвоиться."

echo -e "\n=== Сценарий 3: Консистентность таблиц ==="
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-003", "zone_id": "ZONE-A", "quantity": 100}
}'
sleep 1
echo "Проверка трёх таблиц:"
cqlsh $CASSANDRA_HOST -e "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-003' AND zone_id='ZONE-A';"
cqlsh $CASSANDRA_HOST -e "SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-003';"
cqlsh $CASSANDRA_HOST -e "SELECT * FROM warehouse.inventory_by_zone WHERE zone_id='ZONE-A' AND product_id='SKU-003';"

echo -e "\n=== Сценарий 4: События вне порядка ==="
echo "Отправляем: RECEIVED (100) -> SHIPPED (20) -> RECEIVED (50) с меньшим timestamp (игнорируется)."
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-004", "zone_id": "ZONE-A", "quantity": 100}
}'
sleep 1
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_SHIPPED",
  "data": {"product_id": "SKU-004", "zone_id": "ZONE-A", "quantity": 20}
}'
sleep 1
# Здесь для демонстрации нужно отправить событие с более старым timestamp, но producer этого не поддерживает.
echo "После отправки старого RECEIVED (50) проверьте, что остаток = 80 (не 130). В текущей реализации нет, но логика в коде есть."

echo -e "\n=== Сценарий 5: DLQ ==="
echo "Отправляем невалидное событие (quantity = -5):"
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_SHIPPED",
  "data": {"product_id": "SKU-005", "zone_id": "ZONE-A", "quantity": -5}
}'
sleep 2
echo "Проверьте топик warehouse-events-dlq (например, через kafka-console-consumer)."

echo -e "\n=== Сценарий 6: Отказоустойчивость Cassandra ==="
echo "Остановите одну ноду: docker stop cassandra-2"
echo "Отправьте событие:"
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-006", "zone_id": "ZONE-A", "quantity": 200}
}'
sleep 1
echo "Проверьте, что остаток записался:"
cqlsh $CASSANDRA_HOST -e "SELECT available FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-006' AND zone_id='ZONE-A';"
echo "Запустите ноду обратно: docker start cassandra-2"

echo -e "\n=== Сценарий 7: Мониторинг ==="
echo "Проверьте http://localhost:8080/health – должен вернуть 200 OK"
echo "Проверьте http://localhost:8080/metrics – есть метрики"
echo "Откройте Grafana: http://localhost:3000 (admin/admin)"

echo -e "\n=== Сценарий 8: Schema Evolution ==="
echo "Отправьте V1 события (без supplier_id) и V2 (с supplier_id):"
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-007", "zone_id": "ZONE-A", "quantity": 50}
}'
sleep 1
curl -X POST $PRODUCER_URL/events -H "Content-Type: application/json" -d '{
  "event_type": "PRODUCT_RECEIVED",
  "data": {"product_id": "SKU-008", "zone_id": "ZONE-A", "quantity": 30, "supplier_id": "SUP-001"}
}'
sleep 1
echo "Проверьте supplier_id в таблице:"
cqlsh $CASSANDRA_HOST -e "SELECT product_id, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id IN ('SKU-007','SKU-008');"