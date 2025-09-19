---- To send email from Snowflake, you need to have a verified email. ----
---- In the case of a trail account , this will be the email address that was used to create the account ----
---- Create a email integration that will be used by procedure ----
CREATE or replace NOTIFICATION INTEGRATION PO_EMAIL_INTEGRATION
    TYPE=EMAIL
    ENABLED=TRUE
  ALLOWED_RECIPIENTS=('<your verified email address>');

---- Test the create integration ----
CALL SYSTEM$SEND_EMAIL(
'PO_EMAIL_INTEGRATION',
'<your verified email address>m',
'Hello from Snowflake ',
'My first email sent from Snowflake via SYSTEM$SEND_EMAIL stored procedure');

---- Create a sequence that will be used by procedure to create order numbers ----
CREATE OR REPLACE SEQUENCE seq1;

---- Test the sequence ----
SELECT seq1.NEXTVAL AS po_seq;

---- Create the procedure that will check the stock levels (using PROCEDURE stock_take that was created earlier) , and place a order for all products that fell below their expected quantity ----
---- The email will go to the "supplier" , which is using your verified email address ----
---- Each product order will have a unique number and will have a day time added ----

CREATE OR REPLACE PROCEDURE ICECREAM_STOCK.ICECREAM_STOCK.PO_EMAIL_PROC("EMAIL_INTEGRATION_NAME" VARCHAR, "EMAIL_ADDRESS" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','tabulate')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark
from datetime import datetime
import pandas
def main(
  session: snowflake.snowpark.Session,
  email_integration_name: str,
  email_address: str
) -> str:
    seq_df = session.sql("SELECT seq1.NEXTVAL AS po_seq").collect()
    po_number = seq_df[0]["PO_SEQ"]
    table_pandas_df: pandas.DataFrame = session.table("stock_take_view").to_pandas()
    table_as_html: str = table_pandas_df.to_html()
    now_str = datetime.now().strftime("%d %B %Y %H:%M:%S")
    email_as_html: str = f"""
        <p>Purchase order for Icecream shop  - Carleton, Michigan </p>
        <p><strong>Date/Time:</strong> {now_str}</p>
         <p><strong>PO Number:</strong> PO-{po_number:05d}</p>
        <p>{table_as_html} </p>
        <p><strong>Notes:</strong> This order is to replace any Mismatch stock - Please deliver by end of the day.</p>
        <p> Please phone 555-1093 if you have any questions.</p>
        <p> Thanks for your help.</p>
        <p>Management</p>
        <div style="text-align:center; margin:20px;">
  <!-- Ice Cream Cone -->
  <div style="width:50px; height:80px; background: #D2691E; margin: 0 auto; clip-path: polygon(50% 100%, 0 0, 100% 0);"></div>
    <!-- Ice Cream Scoop -->
  <div style="width:60px; height:60px; background: #FF69B4; border-radius: 50%; margin: -45px auto 0 auto;"></div>
    <!-- Cherry on top -->
  <div style="width:15px; height:15px; background: #FF0000; border-radius: 50%; margin: -50px auto 0 auto;"></div>
  <p style="font-family: Arial, sans-serif; font-size:14px; color:#333; margin-top:10px;">
    Sweet Ice Cream & Hot Drinks!
  </p>
</div>
    """
    success: bool = session.call(
      "SYSTEM$SEND_EMAIL",
      ''PO_EMAIL_INTEGRATION'',
      ''<your verified email address>'',
      ''purchase_order'',
      email_as_html,
      ''text/html''
    )
    return "Email sent successfully" if success else "Sending email failed"
';

---- Test the purchase order email ----
CALL po_email_proc(  'PO_EMAIL_INTEGRATION',  '<your verified email address>');
