FROM hseling/hseling-api-base:python3.6-alpine3.7 as build

LABEL maintainer="Sergey Sobko <ssobko@hse.ru>"

RUN mkdir /dependencies
COPY ./requirements.txt /dependencies/requirements.txt
COPY ./setup.py /dependencies/setup.py

RUN pip install -r /dependencies/requirements.txt

FROM hseling/hseling-api-base:python3.6-alpine3.7 as production

COPY --from=build /usr/local/lib/python3.6/site-packages /usr/local/lib/python3.6/site-packages
COPY --from=build /usr/lib/python3.6/site-packages /usr/lib/python3.6/site-packages

COPY --from=build /dependencies /dependencies

COPY ./hseling_api_template /dependencies/hseling_api_template
RUN pip install /dependencies

COPY ./app /app
