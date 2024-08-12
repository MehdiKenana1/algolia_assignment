FROM apache/airflow:2.9.3

USER root

ENV DEBIAN_FRONTEND=noninteractive \
    TERM=linux \
    AIRFLOW_GPL_UNIDECODE=yes

RUN apt-get -y update \
 && apt-get -y install python3-pip libpq-dev postgresql-client python3-dev python3-distutils python3-apt

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
