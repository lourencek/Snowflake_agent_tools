---- There are 2 emails that will be send - depending on the predicted weather of the following day. ----
---- If tommorows temprature is above 65 fahrenheit , the "hot weather" email is send out to customers. This will give a generated content and a unique promo to drive sales for the day. ----
---- If tommorows temprature is below 65 fahrenheit , the "cold weather" email is send out to customers. This will give a generated content and a unique promo to drive sales for the day. ----
---- The tools interact with the get_forecast_proc tool that was created earlier, and this will dictate which email to send to registered patrons. ----
---- 

---- Create of views that will be used to create content that is used in marketing emails ----
create or replace view hotweather as 
SELECT AI_COMPLETE('claude-3-5-sonnet', 'please write a promotional column (less than 100 words) for a  shop - that offer a 10% discount off icecream "if you give a this icecream fact" to the person at the paypoint. the column should give a icecream fact that needs to be give to the cashier') as "hot weather predicted for tomorrow";

create or replace  view coldweather as 
SELECT AI_COMPLETE('claude-3-5-sonnet', 'please write a promotional column (less than 100 words) for a  shop - that offer a 10% discount off hot beverage "if you give a this hot beverage fact" to the person at the paypoint. the column should give a hot beverage fact that needs to be give to the cashier')  as "cold weather predicted for tomorrow";

---- 
select * from coldweather;
