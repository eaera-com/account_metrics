FROM python:3.12-slim

# Configure Poetry to not create virtual environments
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_NO_INTERACTION=1

COPY pyproject.toml poetry.lock ./
RUN pip install poetry

RUN poetry install --only main --no-interaction --no-ansi --no-root

# Set PYTHONPATH to include the src directory
ENV PYTHONPATH=/app/src

WORKDIR /app
COPY . /app

CMD ["poetry", "run", "pytest"]
