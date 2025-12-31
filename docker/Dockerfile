FROM python:3.14-slim AS base

ARG SETUPTOOLS_SCM_PRETEND_VERSION="0+unknown"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION:-0+unknown}

WORKDIR /build

COPY . .
RUN apt update && apt install -y build-essential git

RUN SETUPTOOLS_SCM_PRETEND_VERSION="$(python - <<'PY'
import os
import re

version = os.environ.get("SETUPTOOLS_SCM_PRETEND_VERSION", "0+unknown").strip()
match = re.match(r"^v?(\\d+\\.\\d+\\.\\d+)-([0-9a-f]+)$", version)
if match:
    version = f"{match.group(1)}.post0+g{match.group(2)}"
elif re.match(r"^v?\\d+\\.\\d+\\.\\d+$", version):
    version = version.lstrip("v")
print(version)
PY
)" pip install --no-cache-dir .
RUN rm -rf /build

WORKDIR /app
RUN mkdir -p /app

FROM base AS api
EXPOSE 8000

ENTRYPOINT ["dev-hops", "api"]
CMD ["--host", "0.0.0.0", "--port", "8000"]



FROM base AS runner
ENTRYPOINT ["dev-hops"]
CMD ["--help"]
