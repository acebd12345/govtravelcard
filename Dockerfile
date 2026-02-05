# Use NVIDIA CUDA base image for GPU support
# Ensure the CUDA version matches what faster-whisper/CTranslate2 expects (usually 11.x or 12.x)
FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04

# Install Python and basic utilities
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY transcribe_app.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Run the application
# host 0.0.0.0 is required for Cloud Run / Docker networking
CMD ["uvicorn", "transcribe_app:app", "--host", "0.0.0.0", "--port", "8080"]