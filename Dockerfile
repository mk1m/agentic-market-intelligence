FROM python:3.11-slim

# Install system dependencies for XGBoost (OpenMP)
RUN apt-get update && apt-get install -y libomp-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run the unified pipeline by default
CMD ["python", "etl_pipeline.py"]