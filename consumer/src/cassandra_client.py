from cassandra.cluster import Cluster, ConsistencyLevel, BatchStatement
from cassandra.query import BatchType
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from datetime import datetime
from config import config
import json
import time
import logging

logger = logging.getLogger(__name__)

class CassandraClient:
    def __init__(self, retries=15, delay=3):
        self.retries = retries
        self.delay = delay
        self.cluster = None
        self.session = None
        self.prepared = {}
        self._connect()

    def _connect(self):
        for attempt in range(self.retries):
            try:
                self.cluster = Cluster(
                    config.CASSANDRA_CONTACT_POINTS,
                    load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
                    protocol_version=5,
                    connect_timeout=10
                )
                self.session = self.cluster.connect(config.CASSANDRA_KEYSPACE)
                self.session.execute("SELECT * FROM inventory_by_product_zone LIMIT 1")
                logger.info(f"Connected to Cassandra (keyspace={config.CASSANDRA_KEYSPACE})")
                self._prepare_statements()
                return
            except Exception as e:
                logger.warning(f"Cassandra connection attempt {attempt+1} failed: {e}")
                if attempt == self.retries - 1:
                    raise
                time.sleep(self.delay)

    def _prepare_statements(self):
        # Upsert основной таблицы
        self.prepared['upsert_inv_product_zone'] = self.session.prepare("""
            UPDATE inventory_by_product_zone
            SET available = ?, reserved = ?, last_updated = ?, supplier_id = ?
            WHERE product_id = ? AND zone_id = ?
        """)
        # Upsert агрегированной таблицы по товару
        self.prepared['upsert_inv_product'] = self.session.prepare("""
            UPDATE inventory_by_product
            SET total_available = ?, total_reserved = ?
            WHERE product_id = ?
        """)
        # Upsert таблицы по зоне
        self.prepared['upsert_inv_zone'] = self.session.prepare("""
            UPDATE inventory_by_zone
            SET available = ?
            WHERE zone_id = ? AND product_id = ?
        """)
        # Чтение текущих значений
        self.prepared['select_inv_product_zone'] = self.session.prepare("""
            SELECT available, reserved FROM inventory_by_product_zone
            WHERE product_id = ? AND zone_id = ?
        """)
        self.prepared['select_inv_product'] = self.session.prepare("""
            SELECT total_available, total_reserved FROM inventory_by_product
            WHERE product_id = ?
        """)
        self.prepared['select_inv_zone'] = self.session.prepare("""
            SELECT available FROM inventory_by_zone
            WHERE zone_id = ? AND product_id = ?
        """)
        # Метаданные
        self.prepared['update_metadata'] = self.session.prepare("""
            UPDATE inventory_metadata
            SET supplier_id = ?, last_updated = ?
            WHERE product_id = ? AND zone_id = ?
        """)
        # Out-of-order и идемпотентность
        self.prepared['update_last_ts'] = self.session.prepare("""
            UPDATE product_last_ts SET last_timestamp = ? WHERE product_id = ?
        """)
        self.prepared['insert_processed'] = self.session.prepare("""
            INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?) USING TTL 86400
        """)
        self.prepared['check_processed'] = self.session.prepare("""
            SELECT event_id FROM processed_events WHERE event_id = ?
        """)
        self.prepared['select_last_ts'] = self.session.prepare("""
            SELECT last_timestamp FROM product_last_ts WHERE product_id = ?
        """)
        # Заказы
        self.prepared['insert_order'] = self.session.prepare("""
            INSERT INTO orders (order_id, status, items, created_at) VALUES (?, ?, ?, ?)
        """)
        self.prepared['update_order_status'] = self.session.prepare("""
            UPDATE orders SET status = ?, completed_at = ? WHERE order_id = ?
        """)
        self.prepared['get_order_items'] = self.session.prepare("""
            SELECT items FROM orders WHERE order_id = ?
        """)

        for stmt in self.prepared.values():
            stmt.consistency_level = ConsistencyLevel.QUORUM

    # --- Атомарное обновление трёх таблиц через logged batch ---
    def _atomic_update(self, product_id, zone_id, total_available_delta, total_reserved_delta,
                       zone_available_delta, supplier_id=None, event_ts=None):
        # Читаем текущие значения
        row_pz = self.session.execute(self.prepared['select_inv_product_zone'], (product_id, zone_id)).one()
        row_p = self.session.execute(self.prepared['select_inv_product'], (product_id,)).one()
        row_z = self.session.execute(self.prepared['select_inv_zone'], (zone_id, product_id)).one()

        cur_available = row_pz.available if row_pz else 0
        cur_reserved = row_pz.reserved if row_pz else 0
        cur_total_available = row_p.total_available if row_p else 0
        cur_total_reserved = row_p.total_reserved if row_p else 0
        cur_zone_available = row_z.available if row_z else 0

        new_available = cur_available + total_available_delta
        new_reserved = cur_reserved + total_reserved_delta
        new_total_available = cur_total_available + total_available_delta
        new_total_reserved = cur_total_reserved + total_reserved_delta
        new_zone_available = cur_zone_available + zone_available_delta

        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM)
        batch.add(self.prepared['upsert_inv_product_zone'],
                  (new_available, new_reserved, event_ts, supplier_id, product_id, zone_id))
        batch.add(self.prepared['upsert_inv_product'],
                  (new_total_available, new_total_reserved, product_id))
        batch.add(self.prepared['upsert_inv_zone'],
                  (new_zone_available, zone_id, product_id))
        self.session.execute(batch)

    # --- Операции с использованием атомарного batch ---
    def receive_product(self, product_id, zone_id, quantity, supplier_id, event_ts):
        self._atomic_update(product_id, zone_id,
                            total_available_delta=quantity,
                            total_reserved_delta=0,
                            zone_available_delta=quantity,
                            supplier_id=supplier_id,
                            event_ts=event_ts)

    def ship_product(self, product_id, zone_id, quantity, event_ts):
        self._atomic_update(product_id, zone_id,
                            total_available_delta=-quantity,
                            total_reserved_delta=0,
                            zone_available_delta=-quantity,
                            supplier_id=None,
                            event_ts=event_ts)

    def move_product(self, product_id, from_zone, to_zone, quantity, event_ts):
        # Чтение from_zone
        row_from = self.session.execute(self.prepared['select_inv_product_zone'], (product_id, from_zone)).one()
        cur_available_from = row_from.available if row_from else 0
        cur_reserved_from = row_from.reserved if row_from else 0
        # Чтение to_zone
        row_to = self.session.execute(self.prepared['select_inv_product_zone'], (product_id, to_zone)).one()
        cur_available_to = row_to.available if row_to else 0
        cur_reserved_to = row_to.reserved if row_to else 0

        new_available_from = cur_available_from - quantity
        new_available_to = cur_available_to + quantity

        batch = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=ConsistencyLevel.QUORUM)
        # from_zone
        batch.add(self.prepared['upsert_inv_product_zone'],
                  (new_available_from, cur_reserved_from, event_ts, None, product_id, from_zone))
        batch.add(self.prepared['upsert_inv_zone'],
                  (new_available_from, from_zone, product_id))
        # to_zone
        batch.add(self.prepared['upsert_inv_product_zone'],
                  (new_available_to, cur_reserved_to, event_ts, None, product_id, to_zone))
        batch.add(self.prepared['upsert_inv_zone'],
                  (new_available_to, to_zone, product_id))
        # total_available не меняется – таблица inventory_by_product не обновляется
        self.session.execute(batch)

    def reserve_product(self, product_id, zone_id, quantity, order_id, event_ts):
        self._atomic_update(product_id, zone_id,
                            total_available_delta=-quantity,
                            total_reserved_delta=quantity,
                            zone_available_delta=-quantity,
                            supplier_id=None,
                            event_ts=event_ts)

    def release_product(self, product_id, zone_id, quantity, order_id, event_ts):
        self._atomic_update(product_id, zone_id,
                            total_available_delta=quantity,
                            total_reserved_delta=-quantity,
                            zone_available_delta=quantity,
                            supplier_id=None,
                            event_ts=event_ts)

    def count_inventory(self, product_id, zone_id, counted_quantity, event_ts):
        row = self.session.execute(self.prepared['select_inv_product_zone'], (product_id, zone_id)).one()
        current = row.available if row else 0
        delta = counted_quantity - current
        if delta != 0:
            self._atomic_update(product_id, zone_id,
                                total_available_delta=delta,
                                total_reserved_delta=0,
                                zone_available_delta=delta,
                                supplier_id=None,
                                event_ts=event_ts)

    def create_order(self, order_id, items, event_ts):
        self.session.execute(self.prepared['insert_order'], (order_id, 'CREATED', json.dumps(items), event_ts))
        for item in items:
            self.reserve_product(item['product_id'], item['zone_id'], item['quantity'], order_id, event_ts)

    def complete_order(self, order_id, event_ts):
        row = self.session.execute(self.prepared['get_order_items'], (order_id,)).one()
        if row:
            items = json.loads(row.items)
            for item in items:
                self.release_product(item['product_id'], item['zone_id'], item['quantity'], order_id, event_ts)
        self.session.execute(self.prepared['update_order_status'], ('COMPLETED', event_ts, order_id))

    # --- Out-of-order и идемпотентность ---
    def get_last_processed_ts(self, product_id):
        row = self.session.execute(self.prepared['select_last_ts'], (product_id,)).one()
        return row.last_timestamp if row else None

    def update_last_ts(self, product_id, ts):
        self.session.execute(self.prepared['update_last_ts'], (ts, product_id))

    def is_event_processed(self, event_id):
        row = self.session.execute(self.prepared['check_processed'], (event_id,)).one()
        return row is not None

    def mark_processed(self, event_id):
        self.session.execute(self.prepared['insert_processed'], (event_id, datetime.utcnow()))