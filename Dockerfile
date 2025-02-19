# -----------------------------------------------------------------------
#           Dockerfile used to configure the web container
# -----------------------------------------------------------------------

# Docker Image with python installed 
FROM python:3-slim

# Sets the working directory and copies the application files.
WORKDIR /app
COPY . /app

# Installation of dependencies
RUN apt-get update
RUN apt-get install -y gcc g++ libffi-dev libssl-dev
RUN apt-get clean

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Streamlit port
EXPOSE 8501

# Command to execute the app
CMD ["streamlit", "run", "app.py"]