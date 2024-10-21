# Latest Python with specific libraries
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY lab_server.py .
COPY log.py .

CMD [ "uvicorn", "lab_server:app", "--reload", "--host","0.0.0.0","--port","8080"]

EXPOSE 8080