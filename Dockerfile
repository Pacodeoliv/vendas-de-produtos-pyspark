FROM python:3.12


WORKDIR /src


COPY poetry.lock pyproject.toml ./


RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev


COPY . .


COPY ./tables/ /src/tables/


ENV PYTHONPATH=/src

CMD ["python", "pyspark-basico.py"]