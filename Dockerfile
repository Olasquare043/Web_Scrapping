FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=10000 \
    DIALOGIC_DASHBOARD_HOST=0.0.0.0 \
    DIALOGIC_DASHBOARD_THREADS=8

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip \
    && python -m pip install -r /app/requirements.txt

COPY . /app

RUN useradd --create-home --shell /bin/bash appuser \
    && mkdir -p /app/runtime_outputs \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 10000

CMD ["python", "run_dialogic_dashboard.py", "--no-browser"]
