-- Database: airflow_etl

-- DROP DATABASE IF EXISTS airflow_etl;


CREATE DATABASE airflow_etl
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False; 
