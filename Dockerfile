# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the Python package source code to the container
COPY . /app

# Install armadillo
RUN apt-get update && apt-get install -y \
    g++ \
    libarmadillo-dev \
    liblapack-dev \
    libblas-dev \
    pkg-config \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*
# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Build and install the Python package
RUN python setup.py install

# Expose the port the app runs on (if applicable)
EXPOSE 80 8080

# Define environment variable
ENV ARMA_INCLUDE_DIR="/usr/include/armadillo"
ENV ARMA_LIB_DIR="/usr/lib/armadillo"

# Set the entry point for running your Python application (if applicable)
# ENTRYPOINT ["python", "your_script.py"]
# Run the Flask application
# CMD ["python3", "scfind_api.py"]

# Install Gunicorn
RUN pip install gunicorn

# Use Gunicorn as the entry point
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "scfind_api:app"]