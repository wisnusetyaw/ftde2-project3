from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def fun_top_countries_get_data(**kwargs):
    # Create a SparkSession
    spark = SparkSession.builder.\
        config("spark.jars.packages", "org.postgresql:postgresql:42.7.0").\
        master("local").\
        appName("PySpark_Postgres").\
        getOrCreate()
    # Load data from the database
    
    # country table
    df_country = spark.read.format('jdbc').\
        option("url", "jdbc:postgresql://34.42.78.12:5434/postgres").\
        option("dbtable", "country").\
        option("user", "airflow").\
        option("password", "airflow").\
        option("driver", "org.postgresql.Driver").\
        load()
    df_country.createOrReplaceTempView("country")
    
    # city table
    df_city = spark.read.format('jdbc').\
        option("url", "jdbc:postgresql://34.42.78.12:5434/postgres").\
        option("dbtable", "city").\
        option("user", "airflow").\
        option("password", "airflow").\
        option("driver", "org.postgresql.Driver").\
        load()
    df_city.createOrReplaceTempView("city")
    
    # Query for task 1
    df_result = spark.sql(
                    '''
                    SELECT
                        co.country,
                        COUNT(ci.country_id) as total,
                        current_date() as date
                    FROM 
                        country as co
                    JOIN 
                        city ci 
                    ON ci.country_id = co.country_id
                    GROUP BY 
                        co.country
                    ORDER BY 
                        total DESC
                    '''
                )

    # Save the result
    df_result.write.mode('append')\
          .partitionBy('date')\
          .option('compression', 'snappy')\
          .format('parquet')\
          .save('data_result_task_1')

def fun_top_countries_load_data(**kwargs):
    df = pd.read_parquet('data_result_task_1')
    engine = create_engine('mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/test')
    df.to_sql('top_country_hilmi', con=engine, if_exists='append')

def fun_total_film_get_data(**kwargs):
    # Create a SparkSession
    spark = SparkSession.builder.\
        config("spark.jars.packages", "org.postgresql:postgresql:42.7.0").\
        master("local").\
        appName("PySpark_Postgres").\
        getOrCreate()
    # Load data from the database
    
    # film_category table
    df_country = spark.read.format('jdbc').\
        option("url", "jdbc:postgresql://34.42.78.12:5434/postgres").\
        option("dbtable", "film_category").\
        option("user", "airflow").\
        option("password", "airflow").\
        option("driver", "org.postgresql.Driver").\
        load()
    df_country.createOrReplaceTempView("film_category")
    
    # category table
    df_country = spark.read.format('jdbc').\
        option("url", "jdbc:postgresql://34.42.78.12:5434/postgres").\
        option("dbtable", "category").\
        option("user", "airflow").\
        option("password", "airflow").\
        option("driver", "org.postgresql.Driver").\
        load()
    df_country.createOrReplaceTempView("category")
    
    # Query for task 2
    df_result = spark.sql(
        '''
        SELECT
            ca.name,
            COUNT(fc.film_id) as total,
            current_date() as date
        FROM
            category as ca
        JOIN
            film_category as fc
        ON
            ca.category_id = fc.category_id
        GROUP BY
            ca.name
        ORDER BY
            total DESC
        '''
    )
    # Save the result
    df_result.write.mode('append')\
          .partitionBy('date')\
          .option('compression', 'snappy')\
          .format('parquet')\
          .save('data_result_task_2')


def fun_total_film_load_data(**kwargs):
    df = pd.read_parquet('data_result_task_2')
    engine = create_engine('mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/test')
    df.to_sql('total_film_hilmi', con=engine, if_exists='append')

with DAG(
    dag_id='d_1_batch_processing',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_top_countries_get_data = PythonOperator(
        task_id='top_countries_get_data',
        python_callable=fun_top_countries_get_data
    )
    
    op_top_countries_load_data = PythonOperator(
        task_id='top_countries_load_data',
        python_callable=fun_top_countries_load_data
    )
    
    op_total_film_get_data = PythonOperator(
        task_id='total_film_get_data',
        python_callable=fun_total_film_get_data
    )
    
    op_total_film_load_data = PythonOperator(
        task_id='total_film_load_data',
        python_callable=fun_total_film_load_data
    )
    
    end_task = EmptyOperator(
        task_id='end'
    )
    
    start_task >> op_top_countries_get_data >> op_top_countries_load_data >> end_task
    start_task >> op_total_film_get_data >> op_total_film_load_data >> end_task
    

    
