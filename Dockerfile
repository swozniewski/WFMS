FROM atlasanalyticsservice/analytics-ingress-base:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

WORKDIR /usr/src/app

COPY Jobs Tasks ./

CMD [ "sleep","9999999" ]