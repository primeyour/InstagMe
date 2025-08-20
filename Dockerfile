# Use a slim Python base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Update the package manager and install ffmpeg
# This is the critical system dependency for video processing
RUN apt-get update && apt-get install -y ffmpeg

# Copy and install Python requirements
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your bot's code
COPY . .

# Command to run your bot
CMD ["bash", "start.sh"]
