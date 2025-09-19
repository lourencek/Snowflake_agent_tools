create database icecream_stock ;
use database icecream_stock ;

create schema icecream_stock ;
use  schema icecream_stock ;

---- Create stock table ----
CREATE OR REPLACE TABLE icecream_shop (
    item_id           STRING    NOT NULL,   -- Unique ID for each product
    item_name         STRING    NOT NULL,   -- Name of the ice cream (e.g., "Vanilla Cone")
    category          STRING,               -- e.g., "Cone", "Cup", "Pint", "Topping"
    quantity_on_hand  INTEGER   NOT NULL,   -- Current stock quantity
    unit_price        NUMBER(10,2),        -- Price per unit
    supplier_name     STRING             -- Name of supplier
 );

---- Insert initial stock data ----
INSERT INTO icecream_shop (item_id, item_name, category, quantity_on_hand, unit_price, supplier_name) VALUES
('IC001', 'Vanilla Cone', 'Cone', 50, 2.50, 'SweetCream Ltd'),
('IC002', 'Chocolate Cone', 'Cone', 45, 4.50, 'SweetCream Ltd'),
('IC003', 'Strawberry Cone', 'Cone', 40, 8.50, 'SweetCream Ltd');

INSERT INTO icecream_shop (item_id, item_name, category, quantity_on_hand, unit_price, supplier_name) VALUES
('HD001', 'Espresso', 'Hot Drink', 30, 3.00, 'CoffeeCo'),
('HD002', 'Cappuccino', 'Hot Drink', 25, 3.50, 'CoffeeCo'),
('HD003', 'Latte', 'Hot Drink', 20, 4.00, 'CoffeeCo');

---- Mimic a businss day , reducing stock with random qunatities for each product ----
---- Note this exlude 'IC003', as this is a non-popular product and serve as a baseline  ----
update icecream_shop set quantity_on_hand =quantity_on_hand - ABS(RANDOM()) % 8  where item_id='IC001';
update icecream_shop set quantity_on_hand =quantity_on_hand - ABS(RANDOM()) % 8 where item_id='IC002';
update icecream_shop set quantity_on_hand =quantity_on_hand - ABS(RANDOM()) % 8  where item_id='HD001';
update icecream_shop set quantity_on_hand =quantity_on_hand - ABS(RANDOM()) % 8 where item_id='HD002';
update icecream_shop set quantity_on_hand =quantity_on_hand - ABS(RANDOM()) % 8 where item_id='HD003';


