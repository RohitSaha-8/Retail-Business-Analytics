# Retail Business Analytics with PySpark

## Introduction:
This project involves the analysis of the "retail_db" dataset, focusing on completed orders and conducting customer and product analytics. Utilizing PySpark, we will execute various tasks to gain valuable insights into customer information, order records, and product details.

## Understanding the Data Model:
The RETAIL_DB database comprises tables such as Department, Customer, Categories, Products, Orders, and Order items. These tables contain essential information for our analytical tasks.

## Project Steps:

### Step 1: Data Upload to HDFS via FTP
1.1 Download the dataset from the "Course Resources" or project description.
1.2 Upload the dataset to the FTP lab from the local system.
1.3 Use the HDFS Web Console and the "put" command to transfer the dataset to HDFS.

### Step 2: PySpark Analysis Tasks
Task 2.1: Logging into PySpark Shell
Login to PySpark shell for further analysis.

Task 2.2: Customer Records Exploration
2.2.1 Show client information for California residents.
2.2.2 Output in text format, saving the results in the specified folder.
2.2.3 Include only records with the state value "CA" and the customer's full name.

Task 2.3: Order Records Exploration
2.3.1 Display all orders with the status "COMPLETE"
2.3.2 Output in JSON format, compressing with GZIP.
2.3.3 Save results in the designated directory, ensuring "order date" is in "YYYY-MM-DD" format.

Task 2.4: Customer Records in a City
2.4.1 Generate a list of consumers residing in the city of "Caguas."
2.4.2 Save the data in a directory with specified requirements (compression and format).

Task 2.5: Category Records Exploration
2.5.1 Save result files in CSV format.
2.5.2 Compress the output using lz4 and store in the designated directory.

Task 2.6: Product Records Analysis
2.6.1 Include only products with a price exceeding 1000.0.
2.6.2 Save output files in parquet format, compressing with snappy.
2.6.3 Remove data with prices less than 1000.0.

Task 2.7: Advanced Product Records Analysis
2.7.1 Include products with a price over 1000.0 and containing the pattern "Treadmill."
2.7.2 Save output files in parquet format, compressing with GZIP.

Task 2.8: Order Records Analysis
2.8.1 Output all PENDING orders in July 2013.
2.8.2 Save data in JSON format, compressing with snappy.
2.8.3 Include only entries with the order status "PENDING" and format order date as "YYYY-MM-DD".

## Conclusion:
By systematically executing these tasks using PySpark, this project aims to provide comprehensive reports on completed orders, customer details, and product analytics. The outcomes will empower the business with valuable insights into its retail operations and enhance decision-making processes.
