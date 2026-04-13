FROM astrocrpublic.azurecr.io/runtime:3.1-14

# install dbt-athena into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-athena-community dbt-postgres dbt-core && deactivate
