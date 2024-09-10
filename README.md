# Batch Processing with Airflow and Spark
This repository contains the code for the blog post [Batch Processing with Airflow and Spark](https://www.ritchievink.com/blog/2019/08/23/batch-processing-with-airflow-and-spark/).

Dataset used in this project is from [DVD Rental DB](https://www.kaggle.com/datasets/kapturovalexander/pagila-postgresql-sample-database).

## Tools
- Apache Airflow (for scheduling and orchestrating the batch processing)
- Apache Spark (for processing the data)
- Docker (for running the services that containing both airflow and spark)
- Postgres (for getting the source data)
- TiDB (for storing the processed data and API integration)

## DAG Graph
The following graph shows the DAG that is used (d_1_batch_processing.py)
![DAG](image.png)

## Task(s)
### Task 1 : Top Country Based on User
- initialize spark session and read the data from the csv file
- get the data from the postgres database using spark.read.format("jdbc") and load the data to the spark dataframe
- group the data by country and count the number of users and sort the data in descending order
- save the data to the parquet file
- load the data from the parquet file and save it to the TiDB
- Integrate the process using airflow DAG and run the task

Table in TiDB (**top_country_hilmi**):

![top_country_hilmi](image-1.png)

### Task 2 : Total Film Based on Category
- initialize spark session and read the data from the csv file
- group the data by category  and count the number of films
- save the data to the parquet file
- load the data from the parquet file and save it to the TiDB
- Integrate the process using airflow DAG and run the task

Table in TiDB (**total_film_hilmi**):

![total_film_hilmi](image-2.png)


## Future Improvements
- Load the parquet to Hadoop HDFS instead of storing it locally and integrate the process using airflow DAG, but the resources are not enough to run the hadoop cluster. 