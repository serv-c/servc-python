ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-alpine AS builder-image
ARG DEBIAN_FRONTEND=noninteractive
ARG WORKDIR=/usr/src/

RUN python -m venv ${WORKDIR}/venv
ENV PATH="${WORKDIR}/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:${PYTHON_VERSION}-alpine AS app
ARG DEBIAN_FRONTEND=noninteractive
ARG WORKDIR=/usr/src/

COPY --from=builder-image ${WORKDIR}/venv ${WORKDIR}/venv


# copy source code
RUN mkdir ${WORKDIR}/app
RUN adduser --system --no-create-home python
COPY servc ${WORKDIR}/app/servc
COPY main.py ${WORKDIR}/app/main.py

# activate nonroot user
USER python
WORKDIR ${WORKDIR}/app

# activate virtualenv
ENV VIRTUAL_ENV=${WORKDIR}/venv
ENV PATH="${WORKDIR}/venv/bin:$PATH"

CMD ["python", "main.py"]
