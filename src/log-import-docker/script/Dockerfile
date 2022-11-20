FROM python:3.8-slim

ENV TZ=Asia/Tokyo

WORKDIR /usr/src/app

RUN apt update -y && apt install curl sudo gnupg openjdk-11-jre -y

RUN curl -L https://toolbelt.treasuredata.com//sh/install-debian-bullseye-td-agent4.sh | sh

RUN python --version && \
    pip --version && \
    pip install --upgrade pip && \
    pip install wheel

COPY . /usr/src/app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python", "log_import.py" ]