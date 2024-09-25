FROM python:3.12-slim

# Configure Poetry to not create virtual environments
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_NO_INTERACTION=1

COPY pyproject.toml poetry.lock ./
RUN pip install poetry

RUN poetry install --only main --no-interaction --no-ansi --no-root

WORKDIR /app
COPY . /app

CMD ["poetry", "run", "python", "-m", "pytest"]
