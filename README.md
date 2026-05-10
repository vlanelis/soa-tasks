# Warehouse

## Запуск

```bash
docker-compose up -d
```

После запуска (через 30–60 секунд) доступны:

- **Producer API** – http://localhost:8000/docs
- **Consumer metrics** – http://localhost:8080/metrics
- **Consumer health** – http://localhost:8080/health
- **Schema Registry** – http://localhost:8081
- **Prometheus** – http://localhost:9090
- **Grafana** – http://localhost:3000 (admin/admin)


---

## Consistency Levels (пункт 8)

Для записи используется QUORUM для устойчивости к падению одного узла.

Для чтения для оптимизации задержки используем ONE, небольшое отставание допустимо.

---

## Эволюция схемы

**Стратегия совместимости:** `BACKWARD` (обратная совместимость) – новая схема может читать данные, записанные старой схемой.

**Пример добавления нового поля** (например, `supplier_id` в событие `PRODUCT_RECEIVED`):

1. Отредактировать Avro-схему `product_received.avsc` – добавить поле с `"default": null`:
   ```json
   {"name": "supplier_id", "type": ["null", "string"], "default": null}
   ```
2. Зарегистрировать новую схему в Schema Registry (это происходит автоматически при первом запуске producer).
3. Обновить consumer: добавить колонку `supplier_id` в таблицу `inventory_metadata` (или основную таблицу) и обрабатывать новое поле.
4. Старые события (без `supplier_id`) будут иметь значение `null`. Новая версия события с заполненным `supplier_id` записывает корректное значение.

---
