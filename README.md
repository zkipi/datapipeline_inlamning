# Datapipeline – Transactions

Detta projekt innehåller en enkel datapipeline som:
- Hämtar transaktionsdata dagligen
- Normaliserar data till två tabeller
- Lagrar data i SQLite
- Körs automatiskt via Airflow
- Visualiseras i Jupyter Notebook

## Struktur
- airflow/dags/ – Airflow DAG
- airflow/scripts/ – ETL-logik och databashantering
- airflow/ – Jupyter Notebook för visualisering

## Krav
- Python 3.9+
- apache-airflow
- pandas
- requests
- matplotlib
