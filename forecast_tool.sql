---- Based on Snowflake Marketplace data "Global Weather & Climate Data for BI" provided by Pelmorex Weather Source ---
---- The below procedure looks for the weather prediction till the next Sunday , on which the shop is closed ----

CREATE OR REPLACE PROCEDURE get_forecast_proc()
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
            date_valid_std,
            avg_temperature_air_2m_f,
            tot_precipitation_in,
            tot_snowfall_in,
            avg_cloud_cover_tot_pct,
            probability_of_precipitation_pct,
            probability_of_snow_pct,
            DATEADD(DAY, 7 - DAYOFWEEKISO(DATEADD(DAY,1,CURRENT_DATE())), DATEADD(DAY,1,CURRENT_DATE())) AS next_sunday
        FROM
            GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.forecast_day
        WHERE
            date_valid_std < NEXT_DAY(CURRENT_DATE, 'SUN')
            AND postal_code='48117'
        ORDER BY date_valid_std
    """).to_pandas()

    # --- Option 1: Return as JSON string ---
    return df.to_json(orient='records', date_format='iso')
$$;

---- Test the forcast proc ---
-- call get_forecast_proc();
