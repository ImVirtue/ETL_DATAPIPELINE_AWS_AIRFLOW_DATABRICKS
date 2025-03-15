# Automated ETL Data pipeline that collects data from API and Processes through several layers before eventually sending it to Databricks

![image](https://github.com/user-attachments/assets/fd128cbd-d7bd-47ca-a23c-ecfb784e8500)

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Conclusion](#concly)

## Introduction
This project is to create an Automated ETL data pipeline that utilize the power of available Cloud services like AWS S3 and Databricks

## Features
- **Automated ETL Pipeline**: Using Apache Airflow to schedule the flow of retrieving data from API, storing it into PostgreSQL and load into the AWS S3 bucket. After that Databricks will detect the new files and
  automatically load into the Databricks layer to process afterwards.
- **Docker Compose Setup**: The project uses Docker Compose to streamline the deployment and management of the required services, including Apache Airflow, and PostgreSQL.

## Technologies Used
- **PostgreSQL**: To store transformed data from API.
- **Apache Airflow**: Schedule tasks from extracting data to loading data to the target source AWS S3.
- **AWS S3 Bucket**: Store data after transforming from API coming under the.csv format
- **Databricks**: Utilize the power of Incremental data processing (Auto Loader) to automatically detect and load data into Delta Lake. After that, Data is transformed within the Multi-Hop Architecture before Reporting or Applying to the Machine Learning model
- **Docker Compose**: To orchestrate the deployment of the above technologies.

## Conclusion
- The main purpose of this project is to create an automated ETL pipeline, it helps understand the interaction between services ranging from open-source to cloud technologies.





