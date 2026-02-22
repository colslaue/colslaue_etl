FROM python:3.10

ENV POETRY_VERSION=2.3.2
RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app

ENTRYPOINT []

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi --no-root

COPY . .