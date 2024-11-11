# Latest Python with specific libraries
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
# COPY requirements.txt .

# Install any dependencies
RUN pip install -U prefect

RUN prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api"

CMD [ "prefect", "server", "start", "--host","0.0.0.0"]

EXPOSE 4200