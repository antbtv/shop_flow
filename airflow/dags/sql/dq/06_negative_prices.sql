-- table: stg_product_versions
-- hint: a product whose current price is negative: check the source row in Postgres
WITH current_prices AS (
    SELECT
        product_id,
        argMax(price, version) AS price,
        argMax(is_deleted, version) AS deleted
    FROM shopflow.stg_product_versions FINAL
    GROUP BY product_id
)

SELECT
    count() AS violations,
    groupArray(10)(toString(product_id)) AS sample_keys -- noqa: LT01
FROM current_prices
WHERE deleted = 0 AND price < 0
