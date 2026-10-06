import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import substring, max, sum, coalesce, col, rank, broadcast, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("AdvancedSparkTraining").master("local[*]").getOrCreate()

'''
Conceptual questions:
Beginner:
1. What is Apache Spark, and why is it faster than Hadoop MapReduce?
    Spark is a data processing tool that works in memory, making it faster than MapReduce because it doesn't have to do constant reads and writes to storage.

2. Name two common use cases for Spark in real-world applications.
    2 common use cases for spark are for performing ETL operations and for spark streaming to process real-time data

3. What are the main components of the Spark ecosystem?
    Spark Core, SQL, Streaming, MLlib and Graphx

4. What is the difference between RDDs and DataFrames?
    DataFrames are suitable for structured data, while RDDs handle semi-structured or unstructured data

5. Explain the concept of lazy evaluation in Spark.
    Lazy evaluation is the concept that transformations won't actually execute until an action is performed.

6. What triggers Spark to actually execute transformations?
    Actions trigger the transformations, such as show() or write()

7. In Spark architecture, what is the role of the Driver and Executors?
    The Driver is what makes the plan and tasks for the Executors to perform.

8. What is Spark SQL, and why would you use it?
    Spark SQL allows you to perform your typical SQL queries on spark Dataframes without needing to load them into a table.

Intermediate:
1. Explain the difference between narrow and wide transformations with examples.
    Wide transformation(such as groupBy and join) require data shuffles which require data to be transfered between executors
    Narrow transformations(such as select() and withColumn()) don't require any data shuffles

2. What is a Spark DAG, and how does it optimize execution?
    The DAG is the logical execution plan spark creates to map the transformations you're making until an action occurs.
    
3. Compare Spark deployment modes: Local, YARN, Standalone, and Kubernetes.
    The deployment modes are different complexities for different uses:
        Local is deployed to your one computer, doesn't need a cluster manager, but it doesn't scale well.
        Standalone has a spark cluster it accesses. It scales better than local and allows sharing of resources.
        YARN runs on a Hadoop/YARN cluster and has a Resource Manager it uses. It works great if you already have a hadoop workload setup you can use.
        Kubernetes is used for cloud and containerised platforms. While it may have the hardest setup to get set up, it makes it versatile and very scalable.

4. When would you use broadcast joins in Spark?
    When you're trying to join a small dataset with a larger dataset.

5. What are the advantages of using Parquet or ORC over CSV in Spark?
    Parquet and ORC are better in Spark because its a columnar format that's optimized for analytics

6. What is the Catalyst Optimizer, and why is it important?
    The catalyst Optimizer is something in spark that helps optimize your code so it executes better and faster.

7. Explain the difference between repartition() and coalesce().
    Using repartition() allows you to spread the data out over a specified number of partitions, coalesce() reduces the number of partitions by compacting the data together.
    repartiton() requires a data shuffle while coalesce() doesn't

8. Why is Spark fault-tolerant?
    Because it keeps a lineage of what changes its making to your RDDs and Dataframes.

Advanced:
1. How does Spark handle stage division in jobs, and how do wide transformations affect this?
    The rule for how many stages there are in a job is the number of wide transformations + 1.

2. Explain the differences between RDD, DataFrame, and Dataset APIs.
    RDDs are meant for semi-structured and unstructured data. 
    Dataframes are for storing structured data.
    Datasets take the optimization and structure of Dataframes and combine it with the compile-time safety of Java and Scala.

3. What is vectorized query execution in Spark, and when is it beneficial?
    Vectorized query execution in Spark allows you to process many values at once in a batch as opposed to processing row by row.

4. Discuss strategies to deal with data skew in Spark joins.
    The first way you should try to fix skew is by making sure AQE is on.
    If you're still getting skew with AQE you need to do Salting.
    
5. How do Spark Window functions differ from groupBy aggregations?
    Window functions still retain all rows and just add a new column with your calculation. The groupBy aggregations actually collapse the rows together based on their shared field.

6. What role do Tungsten and Catalyst play in Spark performance tuning?
    The Catalyst Optimizer optimizes the code to run faster/more efficient and develops a physical plan. This physical plan is given to the Tungsten engine to optimize the low level hardware execution

7. When would you choose Spark Structured Streaming over Spark Streaming?
    Any modern streaming load would be better handled by Structured Streaming because its a more optimized version.

8. How do you tune partition sizes in Spark for optimal performance?
    You want to make sure your data isn't skewed too much because it leads to bottlenecks that cause the execution to take longer. If you can have an equal split of the data across your partitions, it will execute as fast as possible.

'''
# Hands On Exercises
#Beginner
# 1.
employee = spark.read.csv("employees.csv", header=True, inferSchema=True )
# employee.printSchema()
# employee.show(5)

# 2.
# print(employee.select("name").distinct().count())

# 3.
# employee.select("name", "department").show()

# 4.
# employee.filter("salary > 80000").show()

# 5.
# employee.sort("salary", ascending=False).show()

# 6.
# employee.createOrReplaceTempView("dfView")
# spark.sql("SELECT department, AVG(salary) as avg_salary FROM dfView GROUP BY department").show()

# Intermediate
# 1.
student = spark.read.csv("students.csv", header=True, inferSchema=True )
# student.withColumn("firstLetter", substring("name", 1, 1)) \
# .groupBy("firstLetter") \
# .count().alias("letterCount") \
# .show()

# 2.
department = spark.read.csv("departments.csv", header=True, inferSchema=True )
# employee.join(department, on=employee["department"] == department["dept_name"]) \
#     .select(employee["name"], department["id"], department["dept_name"]) \
#     .show()

# 3.
# employee.groupBy(employee["department"]) \
#     .agg(max(employee["salary"])).show()

# 4.
# sales1 = spark.read.csv("sales_2024-01-01.csv", header=True, inferSchema=True )
# sales2 = spark.read.csv("sales_2024-01-02.csv", header=True, inferSchema=True )
# union_sales = sales1.union(sales2)
# union_sales.show()
# union_sales.agg(sum(union_sales["price"])).show()

# 5.
# employee_dedupe = employee.dropDuplicates(["name"])
# employee_dedupe.show()

# 6.
# employee.fillna({"salary": 0}).show()

# 7.
# employee.write.mode("overwrite").option("compression", "snappy").parquet("output/out.parquet")

# 8.
# employee.cache()

# Advanced
# 1.
trans_large = spark.read.csv("transactions_large.csv", header=True, inferSchema=True )
# trans_large.write.mode("overwrite").partitionBy("transaction_date").parquet("output/advanced1/")

# 2.
# wind = Window.partitionBy("department").orderBy("salary")
# employee.withColumn("salary_rank", rank().over(wind)).show()

# 3.
wind = Window.partitionBy("transaction_date").orderBy("amount")
trans_large.withColumn("rolling_avg", avg("amount").over(wind)).show()
# 4.
# employee.join(\
#     broadcast(department), \
#         on=employee["department"] == department["dept_name"]).show()