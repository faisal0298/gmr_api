FROM python:3.8
# FROM python:3.10

# RUN apt-get install build-essential libpoppler-cpp-dev pkg-config python3-dev


RUN apt-get update && apt-get install -y \
    build-essential \
    libpoppler-cpp-dev \
    pkg-config \
    python3-dev \
    libgeos-dev \
    python3-launchpadlib

RUN apt-get update && apt-get install -y libgeos-dev python3-launchpadlib
RUN apt-get install -y software-properties-common


# Install Java
RUN apt-get update && apt-get install -y default-jre

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set LD_LIBRARY_PATH environment variable
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib/server

# Install JPype1
RUN pip3 install JPype1==1.3.0

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

COPY --chown=diycam:diycam service service

COPY --chown=diycam:diycam database database

COPY --chown=diycam:diycam .env .env

ENV PYTHONUNBUFFERED=1

EXPOSE 7704

CMD [ "python3", "main.py" ]
