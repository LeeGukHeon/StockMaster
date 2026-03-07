FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY app ./app
COPY config ./config
COPY docs ./docs
COPY scripts ./scripts

RUN python -m pip install --upgrade pip \
    && python -m pip install -e .

EXPOSE 8501

CMD ["streamlit", "run", "app/ui/Home.py", "--server.address=0.0.0.0", "--server.port=8501"]
