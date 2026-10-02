-- table: dim_customers, dim_products
-- hint: SCD2 must have exactly one current version per live key (ADR-0009):
-- hint: check the order of the journal by version
WITH bad AS (
    SELECT concat('customer:', toString(customer_id)) AS k
    FROM shopflow.dim_customers
    GROUP BY customer_id
    HAVING sum(is_current) > 1
    UNION ALL
    SELECT concat('product:', toString(product_id)) AS k
    FROM shopflow.dim_products
    GROUP BY product_id
    HAVING sum(is_current) > 1
)

SELECT
    count() AS violations,
    groupArray(10)(k) AS sample_keys -- noqa: LT01
FROM bad
