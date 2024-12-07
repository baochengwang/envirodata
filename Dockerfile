#https://medium.com/@albertazzir/blazing-fast-python-docker-builds-with-poetry-a78a66f5aed0
FROM python:3.10

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_OPTIONS_SYSTEM_SITE_PACKAGES=true \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local' \
    POETRY_CACHE_DIR=/tmp/poetry_cache

RUN apt update

RUN apt -y install build-essential curl cmake python3-dev python3-netcdf4 python3-arrow python3-numpy cargo libgdal-dev

RUN curl -sSL https://install.python-poetry.org | python3 -

RUN poetry config --list

COPY pyproject.toml ./
COPY src ./src
COPY config.yaml ./config.yaml
COPY services ./services
COPY static ./static
COPY templates ./templates
RUN touch README.md

RUN poetry install --no-interaction --no-ansi && rm -rf $POETRY_CACHE_DIR

EXPOSE 8000

ENTRYPOINT ["poetry", "run"]
