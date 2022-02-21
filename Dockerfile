FROM atlasanalyticsservice/analytics-ingress-base:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

WORKDIR /home/analyticssvc

RUN mkdir Jobs && mkdir Tasks
COPY Jobs Jobs/
COPY Tasks Tasks/
COPY ca-bundle.trust.crt /etc/pki/tls/certs/ca-bundle.trust.crt

CMD [ "sleep","9999999" ]
