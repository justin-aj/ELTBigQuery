FROM python:3.11-slim

# Set environment variables to prevent Python from writing bytecode and to run in unbuffered mode
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install the dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of your application into the container
COPY . .

# Copy the service account key into the container (use .dockerignore to exclude sensitive files)
COPY ./credentials/service-account.json /app/service-account.json

# Set the environment variable for Google Cloud authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/service-account.json"

# Command to run when the container starts (you can replace this with the actual command for dbt)
CMD ["dbt", "--help"]
