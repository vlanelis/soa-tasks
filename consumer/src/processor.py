import time
from datetime import datetime, timezone
from cassandra_client import CassandraClient
from idempotency import is_processed, mark_processed
from metrics import event_processing_duration

class EventProcessor:
    def __init__(self):
        self.db = CassandraClient()

    def process(self, event, msg) -> bool:
        start_time = time.time()
        event_id = event['event_id']
        event_type = event['event_type']
        data = event['data']
        event_ts_str = event['timestamp']

        try:
            # Преобразуем строку в объект datetime (aware с UTC)
            if event_ts_str.endswith('Z'):
                event_ts_str = event_ts_str[:-1] + '+00:00'
            dt_aware = datetime.fromisoformat(event_ts_str)
            # Приводим к наивному UTC (убираем tzinfo)
            event_ts = dt_aware.replace(tzinfo=None)
        except Exception as e:
            event_ts = datetime.utcnow()

        # Идемпотентность
        if is_processed(event_id):
            return True

        # Out-of-order
        product_id = None
        if event_type in ('PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_MOVED',
                          'PRODUCT_RESERVED', 'PRODUCT_RELEASED', 'INVENTORY_COUNTED'):
            product_id = data.get('product_id')
            if product_id:
                last_ts = self.db.get_last_processed_ts(product_id)
                if last_ts is not None:
                    if event_ts <= last_ts:
                        return True

        if event_type in ('PRODUCT_RECEIVED', 'PRODUCT_SHIPPED', 'PRODUCT_MOVED',
                          'PRODUCT_RESERVED', 'PRODUCT_RELEASED'):
            quantity = data.get('quantity')
            if quantity is not None and quantity <= 0:
                raise ValueError(f"Invalid quantity: {quantity} (must be positive)")

        # 3. Обработка по типу события
        if event_type == 'PRODUCT_RECEIVED':
            self.db.receive_product(
                product_id=data['product_id'],
                zone_id=data['zone_id'],
                quantity=data['quantity'],
                supplier_id=data.get('supplier_id'),
                event_ts=event_ts
            )
        elif event_type == 'PRODUCT_SHIPPED':
            self.db.ship_product(data['product_id'], data['zone_id'], data['quantity'], event_ts)
        elif event_type == 'PRODUCT_MOVED':
            self.db.move_product(data['product_id'], data['from_zone'], data['to_zone'], data['quantity'], event_ts)
        elif event_type == 'PRODUCT_RESERVED':
            self.db.reserve_product(data['product_id'], data['zone_id'], data['quantity'], data['order_id'], event_ts)
        elif event_type == 'PRODUCT_RELEASED':
            self.db.release_product(data['product_id'], data['zone_id'], data['quantity'], data['order_id'], event_ts)
        elif event_type == 'INVENTORY_COUNTED':
            self.db.count_inventory(data['product_id'], data['zone_id'], data['counted_quantity'], event_ts)
        elif event_type == 'ORDER_CREATED':
            self.db.create_order(data['order_id'], data['items'], event_ts)
        elif event_type == 'ORDER_COMPLETED':
            self.db.complete_order(data['order_id'], event_ts)
        else:
            raise ValueError(f"Unknown event type: {event_type}")

        # 4. Отметить обработанным
        if product_id:
            self.db.update_last_ts(product_id, event_ts)
        mark_processed(event_id)
        duration = time.time() - start_time
        event_processing_duration.observe(duration)
        return True