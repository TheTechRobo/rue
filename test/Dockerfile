ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim

RUN pip install rethinkdb

WORKDIR /app
COPY tester.py tester.py

ENTRYPOINT ["python3", "tester.py"]

