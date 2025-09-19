---- First create the view (stock_take_view) that is used by the stocktake prodecure. ----
---- In this view there are expected stock levels , that will determin if a new order needs to be placed with supplier. ----
create or replace view stock_take_view as 
SELECT
    t.item_id,
    t.quantity_on_hand,
    e.expected_quantity,
    CASE 
        WHEN t.quantity_on_hand = e.expected_quantity THEN 'OK'
        ELSE 'Mismatch'
    END AS stock_status
FROM (
    SELECT * FROM icecream_shop
    WHERE item_id IN ('IC001','IC002','IC003','HD001','HD002','HD003')
) t
JOIN (
    -- Expected quantities
    SELECT 'IC001' AS item_id, 50 AS expected_quantity UNION ALL
    SELECT 'IC002', 45 UNION ALL
    SELECT 'IC003', 40 UNION ALL
    SELECT 'HD001', 30 UNION ALL
    SELECT 'HD002', 25 UNION ALL
    SELECT 'HD003', 20
) e
ON t.item_id = e.item_id;

---- To test the view ----
select * from stock_take_view;

---- Create the Stocktake Procedure ----
CREATE OR REPLACE PROCEDURE stock_take()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
import snowflake.snowpark
import pandas as pd
def main(session: snowflake.snowpark.Session) -> str:
    # --- Execute the query ---
    df = session.sql("""
            SELECT
            t.item_id,
            t.quantity_on_hand,
            e.expected_quantity,
            CASE 
                WHEN t.quantity_on_hand = e.expected_quantity THEN 'OK'
                ELSE 'Mismatch'
            END AS stock_status
            FROM (
            SELECT * FROM icecream_shop
            WHERE item_id IN ('IC001','IC002','IC003','HD001','HD002','HD003')
                ) t
            JOIN (
            -- Expected quantities
            SELECT 'IC001' AS item_id, 50 AS expected_quantity UNION ALL
            SELECT 'IC002', 45 UNION ALL
            SELECT 'IC003', 40 UNION ALL
        SELECT 'HD001', 30 UNION ALL
        SELECT 'HD002', 25 UNION ALL
        SELECT 'HD003', 20
        ) e
        ON t.item_id = e.item_id
    """).to_pandas()

    # --- Option 1: Return as JSON string ---
    return df.to_json(orient='records', date_format='iso')
$$;

---- To test the stock_take procedure ----
CALL stock_take();
