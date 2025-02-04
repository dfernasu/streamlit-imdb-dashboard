# -----------------------------------------------------------------------
#           Dockerfile used to configure the web container
# -----------------------------------------------------------------------

# Docker Image with python installed 
FROM python:3

# Sets the working directory and copies the application files.
WORKDIR /app
COPY . /app

# Installation of dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Streamlit port
EXPOSE 8501

# Command to execute the app
CMD ["streamlit", "run", "app.py"]