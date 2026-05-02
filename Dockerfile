FROM python:3.10-slim

WORKDIR /app

# Copy requirements if exists (assuming it exists based on the assignment)
# If not, it will be ignored or we can just copy everything
COPY requirements.txt .
RUN if [ -f "requirements.txt" ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy source code
COPY . .

# Expose default ports (8000-8010) just to be safe
EXPOSE 8000-8010

# Default command
ENTRYPOINT ["python", "main.py"]
