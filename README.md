# ETL_DATAPIPELINE_AWS_AIRFLOW_DATABRICKS

![image](https://github.com/user-attachments/assets/fd128cbd-d7bd-47ca-a23c-ecfb784e8500)

.
├── dags
│   ├── etl_dag.py
│   └── __pycache__
│       ├── etl_dag.cpython-310.pyc
│       └── etl_dag.cpython-39.pyc
├── docker-compose.yml
├── Dockerfile
├── docs (How to get access between Databricks and AWS services)
│   ├── create_instance_profile_to_connect_s3_databricks
│   └── create_secrete_key_to_work_with_s3
├── image
│   └── download.jpeg
├── logs
├── requirements.in
├── requirements.txt
├── script
│   ├── checkpoint.txt
│   ├── config.json (Create on your own)
│   ├── Databricks notebook
│   │   └── Databricks Multi-Hop Architecture process.ipynb
│   ├── entrypoint.sh
│   ├── extract_data.py
│   ├── load_from_db_to_s3.py
│   ├── __pycache__
│   │   ├── extract_data.cpython-310.pyc
│   │   ├── extract_data.cpython-39.pyc
│   │   └── load_from_db_to_s3.cpython-310.pyc
│   └── test_sync.txt
└── tree.txt


