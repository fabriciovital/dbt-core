FROM fabriciovital/data_engineering_stack:spark-master

# Etl Adventure Works

# Use root to set up the environment
USER root

RUN mkdir -p /app

RUN pip install --no-cache-dir python-dotenv

# Setup App Adventure Works
COPY src/notebooks/configs /app/configs/
COPY src/notebooks/functions /app/functions/

# Notebooks
COPY src/notebooks/.env /app/
COPY src/notebooks/106_insert_landing.py /app/
COPY src/notebooks/107_insert_bronze.py /app/
COPY src/notebooks/108_insert_silver.py /app/
COPY src/notebooks/109_insert_gold.py /app/

# Spark Configs
COPY applications/spark/conf/env /env/
COPY applications/spark/conf/util /util/

WORKDIR /app

# Switch to a non-root user for running the application (add a new user if needed)
RUN useradd -ms /bin/bash spark
USER spark