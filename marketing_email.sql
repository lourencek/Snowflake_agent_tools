---- There are 2 emails that will be send - depending on the predicted weather of the following day. ----
---- If tommorows temprature is above 65 fahrenheit , the "hot weather" email is send out to customers. This will give a generated content and a unique promo to drive sales for the day. ----
---- If tommorows temprature is below 65 fahrenheit , the "cold weather" email is send out to customers. This will give a generated content and a unique promo to drive sales for the day. ----
---- The tools interact with the get_forecast_proc tool that was created earlier, and this will dictate which email to send to registered patrons. ----
---- To send email from Snowflake, you need to have a verified email. ----
---- In the case of a trail account , this will be the email address that was used to create the account. ----

---- Create of views that will be used to create content that is used in marketing emails ----
create or replace view hotweather as 
SELECT AI_COMPLETE('claude-3-5-sonnet', 'please write a promotional column (less than 100 words) for a  shop - that offer a 10% discount off icecream "if you give a this icecream fact" to the person at the paypoint. the column should give a icecream fact that needs to be give to the cashier') as "hot weather predicted for tomorrow";

create or replace  view coldweather as 
SELECT AI_COMPLETE('claude-3-5-sonnet', 'please write a promotional column (less than 100 words) for a  shop - that offer a 10% discount off hot beverage "if you give a this hot beverage fact" to the person at the paypoint. the column should give a hot beverage fact that needs to be give to the cashier')  as "cold weather predicted for tomorrow";

---- Test marketing views ----
select * from coldweather;
select * from hotweather;


---- Create 2 procedures for distinct predicted weather conditions ----
CREATE OR REPLACE PROCEDURE po_email_proc_coldweather(
    email_integration_name STRING,
    email_address STRING
) 
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'tabulate')
HANDLER = 'main'
AS $$
import snowflake.snowpark
from datetime import datetime
import pandas
def main(
  session: snowflake.snowpark.Session,
  email_integration_name: str,
  email_address: str
) -> str:
    table_pandas_df: pandas.DataFrame = session.table("coldweather").to_pandas()
    table_as_html: str = table_pandas_df.to_html()
    now_str = datetime.now().strftime("%d %B %Y %H:%M:%S")
    email_as_html: str = f"""
        <p>Marketing for Icecream shop  - Carleton, Michigan </p>
        <p><strong>Date/Time:</strong> {now_str}</p>
        <p>{table_as_html} </p>
        <p> Please phone 555-1093 if you have any questions.</p>
        <p> Thanks for your support.</p>
        <p>Management</p>
        <div style="text-align:center; margin:20px;">
        <img
        class="fit-picture"
        src="https://domf5oio6qrcr.cloudfront.net/medialibrary/14649/20b79d21-c8e2-4bac-8adc-6b8e2cb3c5d6.jpg"
        alt="Grapefruit slice atop a pile of other slices" />
        <!-- Ice Cream Cone -->
        <div style="width:50px; height:80px; background: #D2691E; margin: 0 auto; clip-path: polygon(50% 100%, 0 0, 100% 0);"></div>
  
    <p style="font-family: Arial, sans-serif; font-size:14px; color:#333; margin-top:10px;">
    Sweet Ice Cream & Hot Drinks!
  </p>
</div>
    """
    success: bool = session.call(
      "SYSTEM$SEND_EMAIL",
      'PO_EMAIL_INTEGRATION',
      '<your verified email address>',
      'Marketing : Icecream shop  - Carleton, Michigan ',
      email_as_html,
      'text/html'
    )
    return "Email sent successfully" if success else "Sending email failed"
$$;

CREATE OR REPLACE PROCEDURE po_email_proc_hotweather(
    email_integration_name STRING,
    email_address STRING
) 
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'tabulate')
HANDLER = 'main'
AS $$
import snowflake.snowpark
from datetime import datetime
import pandas
def main(
  session: snowflake.snowpark.Session,
  email_integration_name: str,
  email_address: str
) -> str:
    table_pandas_df: pandas.DataFrame = session.table("hotweather").to_pandas()
    table_as_html: str = table_pandas_df.to_html()
    now_str = datetime.now().strftime("%d %B %Y %H:%M:%S")
    email_as_html: str = f"""
        <p>Marketing for Icecream shop  - Carleton, Michigan </p>
        <p><strong>Date/Time:</strong> {now_str}</p>
        <p>{table_as_html} </p>
        <p> Please phone 555-1093 if you have any questions.</p>
        <p> Thanks for your support.</p>
        <p>Management</p>
        <div style="text-align:center; margin:20px;">
        <img
        class="fit-picture"
        src="https://img.favpng.com/18/8/15/portable-network-graphics-design-clip-art-ice-cream-image-png-favpng-nG2XywwcPyvv4DxkNvVPiUEiQ.jpg"
        alt="Grapefruit slice atop a pile of other slices" />
        <!-- Ice Cream Cone -->
        <div style="width:50px; height:80px; background: #D2691E; margin: 0 auto; clip-path: polygon(50% 100%, 0 0, 100% 0);"></div>
  
    <p style="font-family: Arial, sans-serif; font-size:14px; color:#333; margin-top:10px;">
    Sweet Ice Cream & Hot Drinks!
  </p>
</div>
    """
    success: bool = session.call(
      "SYSTEM$SEND_EMAIL",
      'PO_EMAIL_INTEGRATION',
      '<your verified email address>',
      'Marketing : Icecream shop  - Carleton, Michigan ',
      email_as_html,
      'text/html'
    )
    return "Email sent successfully" if success else "Sending email failed"
$$;

---- Test 2 procedures for distinct predicted weather conditions
CALL po_email_proc_hotweather(  'PO_EMAIL_INTEGRATION',  '<your verified email address>');
CALL po_email_proc_coldweather(  'PO_EMAIL_INTEGRATION',  '<your verified email address>');

