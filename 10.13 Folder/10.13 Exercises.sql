'''
Beginner Conceptual
1. What are the three layers of Snowflakes architecture, and what does each handle?
	Theres a storage layer(Database Layer), a compute layer(Query Processing) and a services layer(Cloud Services).
Storage handles cloud object storage.
Query processing is your virtual warehouses that actually run the queries and do the computations
Cloud Services handles all your overhead getting connected to the cloud.

2. Explain the difference between Permanent, Transient, and Temporary tables.
	Permanent tables are tables that have persist indefinitely until you drop them. They have access to both time-travel for data rollbacks and failsafe to retrieve a table if you accidentally delete it. This is where you would do your typical long-term data storage.
	Transient tables are tables that persist across sessions and have a short time-travel data rollback period. This is good for stuff like holding your intermediate tables while performing an ETL transformation.
	Temporary tables are tables that do session-specific(if you log out and log back in it wont be there anymore). This is the type of table you would store stuff you dont really care about losing because the information isnt important.

3. What is the purpose of a Virtual Warehouse in Snowflake?
	Virtual warehouses are the core of the query processing layer of Snowflake. They act like the computing clusters weve been using with stuff like Spark.

4. How does Snowflake handle semi-structured data like JSON?
	Variant variables

5. Compare Snowpipe with the COPY INTO command.
	this is for tomorrow

    '''

--Beginner Hands on
CREATE DATABASE retail_db;
CREATE SCHEMA q3_sales;

CREATE WAREHOUSE retail_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  COMMENT = 'Training warehouse for Exercise';


create table products(
product_id int,
product_name string,
price float
);

insert into products values
(1, 'pencil', 0.75),
(2, 'paper', 1.00),
(3, 'stapler', 7.50),
(4, 'staples', 2.00),
(5, 'eraser', 1.00)

select * from products;  --validating products got filled

SELECT *
FROM products
WHERE price > 1.00; --switched value to 1.00 because i didn't have anything over 100


'''
Intermediate conceptual
1. What is the difference between Time Travel and Zero-Copy Cloning in Snowflake?
    Time travel allows you to access what the data was at a previous time.
    Zero-copy cloning is pointing towards where data is and keeping track of any changes that happen to the clone specifically on its own.

2. How do Internal Stages differ from External Stages?
    Internal stages are managed by Snowflake while External stages are read-only stages managed by another cloud platform.

3. What role does RBAC play in Snowflake, and why is it important?
    RBAC or Role-Based Access Control controls who whas access to what resources based on roles they can be assigned. 
    This way you can give people access to certain resources, like tables, and restrict them from others, like say the tables for another division. 

4. How do Streams help implement Change Data Capture (CDC)?
    THIS IS FOR TOMORROW

5. Explain how Snowflake achieves separation of compute and storage.
    They are different layers as mentioned above in the Beginner section. Storage is done in the Database Layer while compute is in the Query Processing Layer.
'''

