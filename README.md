# Streamlit IMDB Dashboard

This project consists of a basic Streamlit application, that collects data from a Snowflake data warehouse, to display a series of interactive dashboards built with the Plotly library.

When the user loads a dashboard, the data is downloaded from the Snowflake account and stored in a local PostgresSQL database. This database is used to perform filtering, which is executed each time the user updates the interface filters.

The application runs thanks to 2 Docker containers: one for the Streamlit web and one for the local bd.

## Data origin

The data stored in Snowflake comes from a Kaggle dataset in .csv format, which can be found at the following link:
[IMDB Movies Dataset](https://www.kaggle.com/datasets/yusufdelikkaya/imdb-movie-dataset)

- Although it has only 1000 rows, this dataset has a sufficient number and variety of columns to build a basic star model.
- The original .csv file, can be found inside the repository in the following route:

> ./datasets/imdb_dataset.csv

## Data transformation and upload to Snowflake

To create the star model, the data has been transformed using a python script, which can be found at:

> ./data_transformation.py

Para subir los datos a snowflake, se ha empleado otro script en python, que hace uso del conector oficial de Snowflake para este lenguaje: [Snowflake Connector for Python](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)

> ./load_into_snow.py

In turn, this script uses another SQL script to generate the schema:

> ./scripts/schema_creation.sql

## App Files

This section, covers the use of the rest of python scripts found in the project:

> TODO


