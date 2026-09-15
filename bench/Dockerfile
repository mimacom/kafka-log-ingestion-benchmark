# The load generator, the verifier and the report renderer all run here, so a
# reviewer needs nothing on the host except Docker and Python 3 for the
# scenario drivers.
FROM python:3.12-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /work
