FROM hseling/hseling-api-base:python3.6-alpine3.7

LABEL maintainer="Sergey Sobko <ssobko@hse.ru>"

COPY ./app /app

RUN mkdir /dependencies
COPY ./requirements.txt /dependencies/requirements.txt
COPY ./setup.py /dependencies/setup.py
COPY ./hseling_api_template /dependencies/hseling_api_template

RUN pip install -r /dependencies/requirements.txt
RUN pip install /dependencies
