# Snowflake_ETL

Snowflake ETL pipeline. Load data into tables from stage in snowflake. The star data organization scheme. That data separates to data marts. Orchestrate with Apache Airflow.
The pipeline was developed using docker technology, to start the application you need to write in the console:
```bash
docker compose init
docker compose up
```

## DAGs
Two dags: 
* first extract data from stage, transform data and then load them into tables. 
* second audit all DML operation of tables.

## Photo of ETL dag:

![alt text](https://github.com/arseniIvankovich/Snowflake_ETL/blob/6267352b0f4e4c0236c0f634c4cd0dc3cb06c33c/images_report/etl_dag.jpg)

## Dashboard

The report was compiled based on the data showcases made. The dashboard was made in PowerBI.

Photo of dashboard:

![alt text](https://github.com/arseniIvankovich/Snowflake_ETL/blob/6267352b0f4e4c0236c0f634c4cd0dc3cb06c33c/images_report/report.jpg)
