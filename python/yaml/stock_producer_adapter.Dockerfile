# Use Debian as base image
FROM debian:bullseye-slim

# Set working directory
WORKDIR /app

# Install Python and required system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your application files
COPY stock_producer_adapter.py .
#COPY templates/ templates/
#COPY static/ static/
COPY xml/ xml/
COPY config/stock_producer_adapter_config.ini config/stock_producer_adapter_config.ini

# Expose the port your Flask app runs on
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=stock_producer_adapter
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0"]