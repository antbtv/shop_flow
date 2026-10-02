-- table: stg_inventory
-- hint: stock below zero: the generator sold more than it had,
-- hint: or a write-off was replayed in Postgres
SELECT
    count() AS violations,
    groupArray(10)( -- noqa: LT01
        concat(toString(product_id), ':', toString(warehouse_id))
    ) AS sample_keys
FROM shopflow.stg_inventory FINAL
WHERE is_deleted = 0 AND quantity < 0
