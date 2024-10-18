# ETL Process with Airflow: JSON Log Data to Database

This project automates the ETL (Extract, Transform, Load) process using **Apache Airflow** to load log data from JSON files into a database.

## 📖 Overview

In this ETL pipeline, we automate the ingestion of JSON-formatted log files into a relational database. Airflow orchestrates the execution of each step, ensuring that the process is repeatable, scalable, and easily monitored. 

- **Extract:** Read log data from JSON files.
- **Transform:** Clean, filter, and preprocess the data.
- **Load:** Insert the transformed data into a database.

## 🛠 Technologies

- **Apache Airflow**: Workflow automation and orchestration.
- **Docker**: To containerize Airflow and its dependencies.
- **Python**: For scripting transformations.
- **JSON**: Source data format.
- **Database**: PostgreSQL with tool DBeaver

## 🚀 ETL Pipeline Steps

1. **File Extraction**
   - Extracts JSON files from a designated directory.
   - Uses Airflow sensors to detect new files.

2. **Data Transformation**
   - Processes the JSON data using Python.
   - Cleans and transforms the data.

3. **Data Loading**
   - Inserts the transformed data into a target database table.
   - Handles batch inserts and ensures data integrity.

## Requiment 
- **Python**: Python 10 or newer
- **Docker**: 4.33.1 (need if you not Linux)
- **Airflow**: 2.9.1
- **Libary of Python**: polars, ijson, gzip
- **Process Vietnamese**: https://github.com/enricobarzetti/viet_text_tools

