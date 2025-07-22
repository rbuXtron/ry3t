# Use a specific version of the python:3.9-slim image
FROM python:3.9-slim


# Set the working directory inside the container
WORKDIR /app

# Copy the content of the local src directory to the working directory
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the TCP server script when the container launches
CMD ["python", "automation.py"]