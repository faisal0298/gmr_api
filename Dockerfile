FROM python:3.8

RUN apt-get update && apt-get install -y libgeos-dev

COPY rdx-0.0.4-py3-none-any.whl .

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install rdx-0.0.4-py3-none-any.whl && \
    pip install -r requirements.txt

RUN adduser \
    --disabled-password \
    --gecos "" \
    --no-create-home \
    "diycam"

USER diycam


WORKDIR /home/diycam

COPY --chown=diycam:diycam main.py main.py

COPY --chown=diycam:diycam helpers helpers 

COPY --chown=diycam:diycam database database

ENV PYTHONUNBUFFERED=1

EXPOSE 7704

CMD [ "python3", "main.py" ]
