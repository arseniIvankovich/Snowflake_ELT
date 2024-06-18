# Snowflake_ETL

Snowflake ETL pipeline. Load data into tables from stage in snowflake. The star data organization scheme. That data separates to data marts. All transformations were done using procedures. After that, a secure view is create, where every airport see only data related to them.Orchestrate with Apache Airflow. 
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


## Time-travel feature of Snowflake

Return difference in table contents on specific ID.

```sql
SELECT old.* ,new.*
  FROM PLANES BEFORE(STATEMENT => '01b4fac9-0302-bea8-0002-c4be0002112e') AS old
    FULL OUTER JOIN PLANES AT(STATEMENT => '01b4fac9-0302-bea8-0002-c4be0002112e') AS new
    ON old.id = new.id
  WHERE old.id IS NULL OR new.id IS NULL;
```
Return table contents 1 minute ago.

```sql
SELECT * FROM PLANES AT(offset => -60);
```

Create with table contests of Plane tables committed 5 minutes ago.

```sql
CREATE TABLE restored_table CLONE PLANES AT (OFFSET => -3600);
```

Create database with all object in AIRLINES database existed 5 days ago.

```sql
CREATE DATABASE RESTORED_AIRLINES CLONE RESTORED_AIRLINES
  AT(TIMESTAMP => DATEADD(days, -5, current_timestamp)::timestamp_tz)
  IGNORE TABLES WITH INSUFFICIENT DATA RETENTION;
```