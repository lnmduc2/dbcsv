FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -e .

CMD ["/bin/bash", "-c", "while true; do sleep 3600; done"]