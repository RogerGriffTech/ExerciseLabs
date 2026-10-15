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
    Streams are an object that helps keep track of changes to a table. It does this by tracking INSERTS, UPDATES and DELETES.

5. Explain how Snowflake achieves separation of compute and storage.
    They are different layers as mentioned above in the Beginner section. Storage is done in the Database Layer while compute is in the Query Processing Layer.
    The database layer is stored in other cloud providers like AWS, ADLS or GCS
'''

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
  NULL_IF = ('NULL', 'null', '');

CREATE OR REPLACE STAGE my_stage
COMMENT = 'stage made for the snowflake exercises on 10/13';

show stages;  --check stage was created

--PUT 'file:///C:/Users/Roger Griffin/Documents/Exercises/ExerciseLabs/10.13 Folder/sales.csv' @my_stage;

  CREATE OR REPLACE TABLE sales_fact(
sale_id int,
customer string,
cost float
  )

COPY INTO sales_fact FROM @my_stage/sales.csv
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');
--VALIDATION_MODE = 'RETURN_ERRORS';

select * from sales_fact;

TRUNCATE TABLE customer;
CREATE OR REPLACE TABLE customer(
event_data VARIANT
);

--PUT 'file:///C:/Users/Roger Griffin/Documents/Exercises/ExerciseLabs/10.13 Folder/customers.json' @my_stage;

COPY INTO customer FROM @my_stage/customers.json
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
);

select event_data:first_name as first_name, event_data:last_name as last_name, event_data:city as city, event_data:state as state from customer;

CREATE OR REPLACE STREAM sales_stream ON TABLE sales_fact
APPEND_ONLY = FALSE;

INSERT INTO sales_fact VALUES
(11, 'John Doe', 25.40),
(12, 'Jane Smith', 2.82),
(13, 'Bill Nye', 800.28);

select * from sales_stream;
select * from sales_fact;  --check data in sales_fact already

create or replace table sales_summary(
sale_id int,
customer string,
cost float,
actions string,
isupdate boolean,
row_id string
);

CREATE OR REPLACE TASK update_summary
WAREHOUSE = 'RETAIL_WH'
SCHEDULE = '5 MINUTE'
AS
MERGE INTO sales_summary AS target
using sales_stream AS source
on(target.sale_id = source.sale_id)
--handle matched IDs
WHEN MATCHED THEN
update set target.customer = source.customer, target.cost = source.cost, target.actions = source.metadata$action, target.isupdate = source.metadata$isupdate, target.row_id = source.metadata$row_id
--handle new IDs
WHEN NOT MATCHED THEN
insert(sale_id, customer, cost, actions, isupdate, row_id)
VALUES(source.sale_id, source.customer, source.cost, source.metadata$action, source.metadata$isupdate, source.metadata$row_id)
; -- END CREATE TASK

execute task update_summary; --i did not want to wait 5 minutes each time

SELECT SYSTEM$STREAM_HAS_DATA('SALES_STREAM');
--start and stop the task
ALTER TASK update_summary RESUME;
ALTER TASK update_summary SUSPEND;

select * from sales_summary;

--changes to test stream
DELETE FROM sales_fact where sale_id = 11;
UPDATE sales_fact
SET customer = 'Josh Heart'
WHERE sale_id = 13;


```
Advanced Conceptual
1.In which scenarios would you use Snowpipe Streaming instead of classic Snowpipe?
Snowpipe streaming is for near real-time data streaming, while snowpipe is for batch data ingestion.

2.Describe how to secure shared data using Secure Views and Row Access Policies.
Secure views allow you to hide the logic showing how you get your rows to display when the view is used.
Row Access Policies allow certain roles to only have access to certain rows, ones that are relevant to their use cases.

3.What are some best practices for designing role hierarchies in Snowflake?
Give bare minimum permissions to roles as a base. Higher roles may need higher permissions, give them the least permissions you can for them to be able to do their job.
Seperate access roles from job roles. Give the job roles the necessary access roles so they can perform their jobs.
Keep access granting centralized. Its better to have one place to go to get access to any table as opposed to having to find individual owners of individual permissions you need access to.

4.How does Snowflake handle semi-structured data flattening with LATERAL FLATTEN? Provide anexample.
It allows you to explode out nested values. For example, if you have JSON data, it will make it into 1 new column per element.

5. Explain the billing model in Snowflake — what are the cost drivers for compute vs. storage?
Costs are driven by how much you use the product.
For compute you're going to be looking how big the warehouses are, how big the queries and joins you're doing are, how many tasks you have running, etc.
For storage how much data do you have, are you doing a lot of time-travels and fail-safe executions, possibly a lot of clones being made of tables.

```

--exercises 1 and 2 require AWS accounts which we don't have

DROP TABLE sales_fact;
UNDROP TABLE sales_fact;

create table sales_fact_clone
clone sales_fact;

drop table sales_fact_clone;
select * from sales_fact_clone;


CREATE OR REPLACE MASKING POLICY name_masking AS
(val STRING) returns STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('HR_ROLE', 'ACCOUNTADMIN') then val
    WHEN CURRENT_ROLE() IN ('ENGINEER_ROLE') then '****'
    ELSE 'NO NAMES FOR YOU'
END;

select * from sales_fact;
USE ROLE CA_ROLE;

ALTER TABLE sales_fact MODIFY COLUMN customer 
SET MASKING POLICY name_masking;

--need to unset the masking policy to make changes to see different rows.
ALTER TABLE sales_fact MODIFY COLUMN customer 
UNSET MASKING POLICY;

ALTER TABLE sales_fact ADD COLUMN
state STRING
;

-- adding column to do access control on
update sales_fact set state = 'CA' where sale_id < 10;
update sales_fact set state = 'GA' where (sale_id >= 10);

CREATE OR REPLACE TABLE state_access_map(
user_role VARCHAR,
state VARCHAR
);
-- setting up roles needed for this lab
CREATE ROLE CA_ROLE;
GRANT USAGE ON DATABASE retail_db
    TO ROLE CA_ROLE;
GRANT USAGE ON SCHEMA q3_sales
    TO ROLE CA_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA q3_sales
    TO ROLE CA_ROLE;
    
CREATE ROLE GA_ROLE;
GRANT USAGE ON DATABASE retail_db
    TO ROLE GA_ROLE;
GRANT USAGE ON SCHEMA q3_sales
    TO ROLE GA_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA q3_sales
    TO ROLE GA_ROLE;

INSERT INTO state_access_map VALUES
('CA_ROLE', 'CA'),
('GA_ROLE', 'GA');

CREATE OR REPLACE ROW ACCESS POLICY state_policy
AS(state_col VARCHAR) RETURNS BOOLEAN ->
CASE
WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
WHEN EXISTS(
    SELECT 1 FROM state_access_map
    WHERE user_role = CURRENT_ROLE()
    AND state = state_col
) THEN TRUE
ELSE FALSE
END;

  ALTER TABLE sales_fact  ADD ROW ACCESS POLICY state_policy on (state);