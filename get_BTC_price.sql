---- The Snowflake External Access Integration with Python Stored Procedures provides a secure way to connect Snowflake to external APIs. ----
---- By combining a network rule to specify the allowed external host, a secret to safely store the API key, and  ----
---- an external access integration to link them, you can make secure API calls without exposing credentials. ----
---- API provider used = https://api.api-ninjas.com/v1/bitcoin ----
---- NOTE : External access is NOT availble to trial accounts ----

---- Create all the supporting Snowflake objects  ----
--a) NETWORK RULE
CREATE OR REPLACE NETWORK RULE my_api_bt
      MODE = EGRESS
      TYPE = HOST_PORT
      VALUE_LIST = ('api.api-ninjas.com');
--b) Secret
Create secret my_api_bt_secret
      type = generic_string
      secret_string = '<your API key>';
--c) EXTERNAL ACCESS INTEGRATION
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION my_external_integration_bt
      ALLOWED_NETWORK_RULES = (my_api_bt)
      ALLOWED_AUTHENTICATION_SECRETS = (my_api_bt_secret) 
      ENABLED = TRUE;

---- Create procedure ----
CREATE OR REPLACE PROCEDURE GET_PRICE_FROM_API()
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python','requests')
EXTERNAL_ACCESS_INTEGRATIONS = (my_external_integration_bt)
SECRETS = ('api_key' = my_api_bt_secret)
HANDLER = 'get_price'
AS
$$
import requests
import _snowflake

def get_price(session):
    # Retrieve API key from Snowflake secret
    api_key = _snowflake.get_generic_secret_string("api_key")
    # Build request (hard-coded to bitcoin endpoint)
    url = "https://api.api-ninjas.com/v1/bitcoin"
    headers = {"X-Api-Key": api_key}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code} {response.text}")
    data = response.json()
    # Extract "price" field
    price = data.get("price")
    return float(price) if price is not None else None
$$;

---- To test the GET_PRICE_FROM_API procedure ----
call GET_PRICE_FROM_API();

