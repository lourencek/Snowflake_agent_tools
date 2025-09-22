---- Create store procedure that will  that is used by the stocktake prodecure that have access to a machine learning model  ----
---- Polynomial Regression, is a machine learning model that extends linear regression to handle non-linear relationships between independent and dependent variables. ----
CREATE OR REPLACE PROCEDURE PREDICT_SALES_FROM_WEATHER_JSON()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python','scikit-learn','pandas','numpy')
HANDLER = 'predict_sales'
AS
$$
import pandas as pd
import json
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
def predict_sales(session):
    # 1. Load historical sales data
    sales_df = session.table("SALES_OVER_TIME").to_pandas()
    sales_df = sales_df[sales_df['NUMBER_ICECREAM_SOLD'] > 0]
    # 2. Define features and target
    X = sales_df[['AVG_TEMPERATURE']]
    y = sales_df['NUMBER_ICECREAM_SOLD']
    # 3. Train polynomial regression model
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    poly_model = make_pipeline(PolynomialFeatures(degree=2), LinearRegression())
    poly_model.fit(X_train, y_train)
    # 4. Get the latest forecasted temperature
    weather_df = session.sql("""
        SELECT date_valid_std, avg_temperature_air_2m_f
        FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.FORECAST_DAY
        WHERE postal_code = '48117'
        ORDER BY date_valid_std
        LIMIT 1
    """).to_pandas()
    forecast_date = weather_df['DATE_VALID_STD'].iloc[0]
    forecast_temp = weather_df['AVG_TEMPERATURE_AIR_2M_F'].iloc[0]
    # 5. Predict sales and round to nearest integer
    input_df = pd.DataFrame({'AVG_TEMPERATURE': [forecast_temp]})
    prediction = poly_model.predict(input_df)
    predicted_sales_rounded = round(float(prediction[0]))
    # 6. Prepare JSON result
    result_dict = {
        "date_valid_std": str(forecast_date),  # convert date to string
        "forecast_temperature": float(forecast_temp),
        "predicted_sales": predicted_sales_rounded
    }
    return json.dumps(result_dict)  # return as JSON string
$$;

---- To test the stock_take procedure ----
CALL PREDICT_SALES_FROM_WEATHER_JSON();
