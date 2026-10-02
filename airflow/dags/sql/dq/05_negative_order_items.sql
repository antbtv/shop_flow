-- table: stg_order_items
-- hint: quantity <= 0 or a negative price in a live order line:
-- hint: check the generator or the source row in Postgres
-- FR-9 "no negative amounts". quantity is Int32 like Postgres INT, so a negative value is
-- visible here instead of overflowing (ADR-0008).
SELECT
    count() AS violations,
    groupArray(10)(toString(order_item_id)) AS sample_keys -- noqa: LT01
FROM shopflow.stg_order_items FINAL
WHERE is_deleted = 0 AND (quantity <= 0 OR price_at_order < 0)
