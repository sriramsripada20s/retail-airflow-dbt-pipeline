FROM astrocrpublic.azurecr.io/runtime:3.3-4
RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir soda-core-snowflake && deactivate