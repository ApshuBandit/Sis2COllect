# Krisha Apartment Scraper & ETL Pipeline

## Project Overview

This project is an end-to-end data pipeline for scraping, cleaning, and loading apartment rental data from [Krisha.kz](https://krisha.kz), a popular real estate website in Kazakhstan. The pipeline is designed to automate data collection and processing to facilitate analytics, reporting, and database storage.

The workflow includes the following steps:

1. **Scraping**: Collect apartment listings, including price, location, number of rooms, area, floor, and URL, using Selenium and Python.
2. **Data Cleaning**: Standardize and normalize data using a custom `DataCleaner` class. This includes:
   - Extracting numeric values from text fields.
   - Parsing titles to separate rooms, area, and floor information.
   - Splitting location into district and address.
   - Filling missing values (currency: KZT, city: Almaty).
3. **Loading**: Insert cleaned data into a SQLite database (`krisha_apartments.db`) using a `DataLoader` class.
   - Handles duplicates and updates existing records.
   - Provides statistics and verification of the database content.
4. **Automation (Optional)**: The pipeline can be scheduled and orchestrated using Apache Airflow with daily execution, either locally or via Docker.

---

How to Run
1. Scraping
python src/krisha_scraper.py


This script scrapes apartment listings from Krisha.kz and saves the raw data as src/data/krisha_apartments.csv.

2. Cleaning
python src/cleaner.py


Cleans the raw data, parses titles into numeric fields, fills missing values, and saves src/data/cleaned_apartments.csv.

3. Loading
python src/loader.py


Loads the cleaned data into krisha_apartments.db, verifies, and optionally exports back to CSV.

4. Full Pipeline (Optional)
python src/krisha_pipeline.py


Executes scraping → cleaning → loading in one script.

Automation with Airflow (Optional)

Install Airflow (Python 3.11 recommended):

pip install "apache-airflow[selenium]==2.7.2"


Create a DAG to run the full pipeline once per day.

Start Airflow scheduler and webserver:

airflow db init
airflow scheduler
airflow webserver


Monitor and manage DAG runs via the Airflow UI (http://localhost:8080).

Notes

Currency is automatically set to KZT, and city is set to Almaty if missing.

The pipeline handles duplicate URLs and updates existing records in the SQLite database.

The workflow is designed to be modular, allowing updates to scraping, cleaning, or loading independently.
