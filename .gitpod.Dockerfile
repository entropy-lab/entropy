FROM gitpod/workspace-full

# Make poetry install under .venv in the project
ENV POETRY_VIRTUALENVS_IN_PROJECT=true
# By default this is `yes` breaking the poetry installation
ENV PIP_USER=false

RUN python3 -m pip install --no-cache-dir poetry && \
    python3 -m pip install --no-cache-dir poethepoet