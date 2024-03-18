Upload the dataset to “HDFS” from FTP :
hdfs dfs -put Retail_Business/data-files/customers-tab-delimited Retail_Business
hdfs dfs -put Retail_Business/data-files/orders_parquet Retail_Business
hdfs dfs -put Retail_Business/data-files/categories Retail_Business
hdfs dfs -put Retail_Business/data-files/products_avro Retail_Business

Task 2.2 --> Explore the customer records saved in the "customers-tab-delimited" directory on HDFS
customer_df = spark.read.format("csv").option("delimiter", "\t").option("header", "false").option("inferSchema", "true").load("Retail_Business/customers-tab-delimited/part-m-00000").toDF("customer_id","customer_fname","customer_lname","customer_email" ,"customer_password" ,"customer_street" ,"customer_city" ,"customer_state" ,"customer_zipcode")

2.2.1 Show the client information for those who live in California
2.2.2 Save the results in the result/scenario1/solution folder
2.2.3 Only records with the state value "CA" should be included in the result
2.2.4 Only the customer's entire name should be included in the output
Example: “Robert Hudson”

from pyspark.sql.functions import col,concat,lit,from_unixtime,date_format
df_california = customer_df.withColumn('customer_fullname',concat('customer_fname', lit(' '), 'customer_lname')).drop('customer_fname', 'customer_lname').filter(col("customer_state") == 'CA')
save dataframe in hdfs: df_california.write.format("csv").option("delimiter", "\t").option("header", "true").save("Retail_Business/result/scenario1/solution")

Task 2.3 --> Explore the order records saved in the “orders parquet” directory on HDFS

order_df = spark.read.parquet(r'Retail_Business/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet')

2.3.1 Show all orders with the order status value "COMPLETE"
2.3.2 The output should be in JSON format
2.3.3 Save the data in the "result/scenario2/solution" directory on HDFS
2.3.4 The "order date" column should be in the "YYYY-MM-DD" format
2.3.5 Use GZIP compression to compress the output
2.3.6 Only the column names listed below should be included in the output:
2.3.6.1 Order number
2.3.6.2 Order date
2.3.6.3 Current situation

convert epoch time(in ms) to date format :
orderdate_df = order_df.withColumn("order_date", from_unixtime(col("order_date")/1000, 'yyyy-MM-dd'))
complete_order_df = orderdate_df.filter(col("order_status") == 'COMPLETE').select("order_id","order_date","order_status")
save dataframe in hdfs: complete_order_df.write.format("json").option("compression", "gzip").option("header", "true").save("Retail_Business/result/scenario2/solution")

Task 2.4 --> Explore the customer records saved in the "customers-tab-delimited" directory on HDFS
2.4.1 Produce a list of all consumers who live in the city of "Caguas"
2.4.2 Save the data in the result/scenario3/solution directory on HDFS
2.4.3 The result should only contain records with the value "Caguas" for the customer city
2.4.4 Use snappy compression to compress the output
2.4.5 Save the file in the orc format

df_Caguas = customer_df.filter(col("customer_city") == 'Caguas')
save dataframe in hdfs: df_Caguas.write.format("orc").option("compression", "snappy").option("header", "true").save("Retail_Business/result/scenario3/solution")

Task 2.5 --> Explore all the category records stored in the “categories” directory on HDFS 2.5.1 Save the result files in CSV format 2.5.2 Save the data in the result/scenario4/solution directory on HDFS 2.5.3 Use lz4 compression to compress the output

category_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").option("inferSchema", "true").load("Retail_Business/categories/part-m-00000").toDF("category_id","category_department_id","category_name")
save dataframe in hdfs: category_df.write.format("csv").option("compression", "lz4").option("header", "true").save("Retail_Business/result/scenario4/solution")

Task 2.6 --> Explore all product records that are saved in the “products_avro” database
2.6.1 Only products with a price of more than 1000.0 should be included in the output
2.6.2 Save the output files in parquet format
2.6.3 Remove data from the table if the product price is lesser than 1000.0
2.6.4 Save the data in the result/scenario5/solution directory on HDFS
2.6.5 Use snappy compression to compress the output

products_df = spark.read.format("avro").load("Retail_Business/products_avro")
df_prod1000 = products_df.filter(col("product_price") > 1000)
save dataframe in hdfs: df_prod1000.write.parquet("Retail_Business/result/scenario5/solution", compression="snappy")

Task 2.7 --> Explore the “products_avro” stored in product records
2.7.1 Only products with a price of more than 1000.0 should be in the output
2.7.2 The pattern "Treadmill" appears in the product name
2.7.3 Save the output files in parquet format
2.7.4Save the data in the result/scenario6/solution directory on HDFS
2.7.5 Use GZIP compression to compress the output

df_prod = products_df.filter(col("product_price") > 1000).filter(col("product_name").like("%Treadmill%"))
save dataframe in hdfs: df_prod.write.parquet("Retail_Business/result/scenario6/solution", compression="gzip")

Task 2.8 --> Explore the order records that are saved in the “orders parquet” table on HDFS
2.8.1 Output all PENDING orders in July 2013
2.8.2 Output files should be in JSON format
2.8.3 Save the data in the result/scenario7/solution directory on HDFS.
2.8.4 Only entries with the order status value of "PENDING" should be included in the result
2.8.5 Order date should be in the YYYY-MM-DD format
2.8.6 Use snappy compression to compress the output, which should just contain the order date and order status

final_df = orderdate_df.filter(col("order_date").like("2013-07%")).filter(col("order_status") == 'PENDING').select("order_date", "order_status").orderBy("order_date")
save dataframe in hdfs: final_df.write.format("json").option("compression", "snappy").option("header", "true").save("Retail_Business/result/scenario7/solution")