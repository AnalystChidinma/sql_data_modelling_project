/*
-----------------------------------------------------------------------------------------------------------------------
Creating a star schema
-----------------------------------------------------------------------------------------------------------------------
The purpose of this script is to build a basic data warehouse schema by transforming raw transactional sales data (raw_customer_orders) 
into a structured dimensional model suitable for analytical querying and reporting. 
i created Dim_product table, dim_customer and the fact table using postgresql.

warning: runing all the query, will drop all the entire table of the schema if it exist.
kindly proceed with caution
*/

select *
FROM datagirl.raw_customer_orders;

--- product diamension table

drop table if exists dim_product;

create table datagirl.dim_product(
product_key serial primary key,
product_category Varchar (255),
product_name varchar (255)
);

insert into dim_product (
product_category, product_name)
select distinct product_category, product_name
from customer_orders co ;

select  * from dim_product dp ;

-- check for duplicate --

select product_key,product_category,product_name, count (*)
From dim_product
group by product_key ,product_category,product_name
having count (*) > 1;

--- dim_customer table

drop table if exists dim_customer;

create table datagirl.dim_customer(
customer_key serial primary key, --- auto increment 
 customer_id varchar (255)
);

insert into dim_customer(customer_id)
select distinct customer_id from raw_customer_orders;

select * from dim_customer 

--- check for duplicate ---
  
select customer_id, count (*)
From dim_customer
group by customer_id
having count (*) > 1;

--- create fact customer table

drop table if exists datagirl.fact_customer_orders;

create table datagirl.fact_customer_orders(
order_id varchar (225),
product_key serial,
customer_key serial,
order_date varchar (225),
quantity int,
price_per_unit int,
discount_applied int
);

INSERT INTO fact_customer_orders (
	order_id,
	product_key,
	customer_key,
    order_date,
    quantity, 
    price_per_unit, 
    discount_applied
)
SELECT 
	raw_customer_orders.order_id,
   dim_product.product_key,
   dim_customer.customer_key,
   raw_customer_orders.order_date,
   raw_customer_orders.quantity,
    raw_customer_orders.price_per_unit,
    raw_customer_orders.discount_applied
        
FROM raw_customer_orders
LEFT JOIN dim_customer 
    ON dim_customer.customer_id = raw_customer_orders.customer_id 
LEFT JOIN dim_product 
    ON dim_product.product_category = raw_customer_orders.product_category 
    
    --- testing for missing key---
    
    select * from fact_customer_orders
    where product_key is null and customer_key is null;
    
  --- check for duplicate in the data

 SELECT 
   customer_key, product_key, order_id, quantity, price_per_unit, discount_applied, COUNT(*) 
FROM fact_customer_orders
GROUP BY 
  customer_key, product_key, order_id, quantity, price_per_unit, discount_applied
HAVING COUNT(*) > 1;

--- alter the fact table to add the column for total amount of sales

alter table fact_customer_orders add column amount decimal (17,2); 

select * from fact_customer_orders order by order_id;
  
update fact_customer_orders 
set amount = (quantity * price_per_unit)-discount_applied;

-- using aggregate function to group my fact table--

select order_id, customer_key, COUNT(product_key) as product_key, order_date, sum(price_per_unit) as total_unit_price, SUM(quantity) as total_quantity,
  sum(amount) as total_sales_amount,sum(discount_applied) as total_discount
    from fact_customer_orders
  group by order_id, customer_key, order_date
  order by order_id, order_date;
