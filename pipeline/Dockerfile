From python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

#Run pip install pandas pyarrow

Workdir /code



COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

Copy pipeline.py .

ENTRYPOINT ["uv","run","python", "pipeline.py"]
