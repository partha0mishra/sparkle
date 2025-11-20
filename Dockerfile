# Dockerfile for Sparkle Framework
# Based on Apache Spark 3.5 with Delta Lake, MLflow, and all dependencies

FROM bitnami/spark:3.5.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    git \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip setuptools wheel

# Install Delta Lake
RUN pip3 install \
    delta-spark==2.4.0 \
    pyspark==3.5.0

# Install ML dependencies
RUN pip3 install \
    mlflow==2.9.0 \
    scikit-learn==1.3.2 \
    xgboost==2.0.2 \
    lightgbm==4.1.0 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    pyarrow==14.0.1

# Install data connectors
RUN pip3 install \
    psycopg2-binary==2.9.9 \
    pymongo==4.6.0 \
    redis==5.0.1 \
    kafka-python==2.0.2 \
    boto3==1.34.8 \
    google-cloud-storage==2.13.0 \
    azure-storage-blob==12.19.0 \
    snowflake-connector-python==3.6.0 \
    requests==2.31.0

# Install data quality and validation
RUN pip3 install \
    great-expectations==0.18.7 \
    pydantic==2.5.3 \
    jsonschema==4.20.0

# Install utilities
RUN pip3 install \
    python-dotenv==1.0.0 \
    click==8.1.7 \
    pyyaml==6.0.1 \
    jinja2==3.1.2 \
    faker==21.0.0

# Install testing dependencies
RUN pip3 install \
    pytest==7.4.3 \
    pytest-spark==0.6.0 \
    pytest-cov==4.1.0 \
    pytest-mock==3.12.0 \
    coverage==7.3.4

# Install Streamlit for demo
RUN pip3 install \
    streamlit==1.29.0 \
    plotly==5.18.0 \
    altair==5.2.0

# Copy Sparkle framework
WORKDIR /opt/sparkle
COPY . /opt/sparkle/

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/sparkle"
ENV SPARK_HOME=/opt/bitnami/spark

# Configure Spark for Delta Lake
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_CONF_DIR/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_CONF_DIR/spark-defaults.conf

# Create data directories
RUN mkdir -p /opt/spark-data/bronze /opt/spark-data/silver /opt/spark-data/gold /opt/spark-data/ml

# Set permissions
RUN chown -R 1001:1001 /opt/sparkle /opt/spark-data

USER 1001

CMD ["bash"]
