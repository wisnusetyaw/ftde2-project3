# Session 18 - Project 3 Batch Processing Using Airflow and Spark

Use case:

From product need help to integrate data from our dwh to their product via API:

- Top Country Based on User
- Total Film Based on Category

Prepare Tools:

- Airflow on Local
- VSCode
- Dbeaver
- Register postman
    - https://www.postman.com/
- Docker pull
    - `docker pull apache/airflow:2.10.0`
    - `docker pull postgres:13`
- Please clone this repo https://github.com/MSinggihP/airflow-docker and do step below:
    - `docker build -t my-airflow .`
    - `docker compose up -d`

Dataset:

https://www.kaggle.com/datasets/kapturovalexander/pagila-postgresql-sample-database
