# syntax=docker/dockerfile:1
# The deltaflow worker. Dependencies are installed exactly as uv.lock pins
# them; the image runs as an unprivileged user and keeps its state in
# /var/lib/deltaflow (mount a persistent volume there).
#
#   docker build -t deltaflow .
#   docker run --rm -v deltaflow-state:/var/lib/deltaflow -v $PWD/views.sql:/views.sql:ro \
#     -e DELTAFLOW_DSN=postgresql://... -p 9187:9187 \
#     deltaflow run /views.sql --slot shop --state-dir /var/lib/deltaflow/shop --http-host 0.0.0.0

FROM python:3.12-slim AS build
RUN pip install --no-cache-dir uv==0.12.5
WORKDIR /src
COPY pyproject.toml uv.lock README.md ./
COPY deltaflow ./deltaflow
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt -o requirements.txt \
 && uv venv /opt/deltaflow \
 && uv pip install --python /opt/deltaflow/bin/python --require-hashes -r requirements.txt \
 && uv pip install --python /opt/deltaflow/bin/python --no-deps .

FROM python:3.12-slim
RUN groupadd --system --gid 10001 deltaflow \
 && useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/deltaflow --create-home deltaflow
COPY --from=build /opt/deltaflow /opt/deltaflow
ENV PATH=/opt/deltaflow/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER deltaflow
WORKDIR /var/lib/deltaflow
VOLUME /var/lib/deltaflow
EXPOSE 9187
# The monitoring server answers /healthz while the process runs (it listens
# on 127.0.0.1:9187 by default, so this works without --http-host).
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s CMD \
  python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9187/healthz', timeout=2)"
ENTRYPOINT ["deltaflow"]
CMD ["--help"]
