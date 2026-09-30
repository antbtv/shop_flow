-- SCD2 dimension products (FR-3), rebuilt every 2 minutes from the journal stg_product_versions
-- (ADR-0009). Owns dim_products: a refresh swaps the whole table (EXCHANGE), direct writes vanish.
-- 1) changes: journal after FINAL; a row whose (attributes, is_deleted) equal the previous row
--    of the key does not open a version (snapshot repeats, updates of other columns).
--    Windows are ordered by version (LSN), not valid_from: the laptop clock may step back.
-- 2) versions: valid_to = valid_from of the next kept row; the explicit frame is required for
--    leadInFrame, toNullable makes the last row NULL instead of 1970-01-01.
-- 3) delete rows only close the previous version. The first version from a snapshot opens at
--    1970-01-01: the row existed before the snapshot, orders older than it must find it.
CREATE MATERIALIZED VIEW IF NOT EXISTS shopflow.dim_products_mv -- noqa: PRS
REFRESH EVERY 2 MINUTE SETTINGS refresh_retries = 3
TO shopflow.dim_products
AS
WITH
changes AS (
    SELECT
        product_id,
        valid_from,
        name, category, price,
        is_snapshot,
        is_deleted,
        version,
        lagInFrame((name, category, price, is_deleted)) OVER w AS prev_attrs,
        row_number() OVER w AS rn
    FROM shopflow.stg_product_versions FINAL
    WINDOW w AS (
        PARTITION BY product_id ORDER BY version
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

kept AS (
    SELECT *
    FROM changes
    WHERE rn = 1 OR (name, category, price, is_deleted) != prev_attrs
),

versions AS (
    SELECT
        product_id,
        name, category, price,
        is_deleted,
        version,
        if(rn = 1 AND is_snapshot = 1, toDateTime64(0, 6, 'UTC'), valid_from) AS opened_at,
        leadInFrame(toNullable(valid_from)) OVER (
            PARTITION BY product_id ORDER BY version
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS closed_at
    FROM kept
)

SELECT
    product_id,
    name, category, price,
    opened_at AS valid_from,
    closed_at AS valid_to,
    toUInt8(closed_at IS NULL) AS is_current,
    version
FROM versions
WHERE is_deleted = 0
SETTINGS max_memory_usage = 536870912, max_threads = 2;
