FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /opt/airflow

# Copy dependency metadata first for better build caching
COPY pyproject.toml uv.lock .python-version ./

# Create project virtualenv with uv
RUN uv sync --locked

# Copy your project code
COPY app/ ./app/
COPY streamlit/ ./streamlit/
COPY dbt_police_data/ ./dbt_police_data/
COPY airflow/dags/ ./dags/
COPY airflow/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENV PATH="/opt/airflow/.venv/bin:$PATH"

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
CMD ["airflow", "standalone"]