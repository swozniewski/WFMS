FROM atlasanalyticsservice/analytics-ingress-base:latest

LABEL maintainer Sebastian Wozniewski <sebastian.wozniewski@cern.ch>

WORKDIR /home/analyticssvc

USER root
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
RUN yum update -y
RUN yum -y install postfix mailx
RUN mv /etc/mail.rc /home/analyticssvc/mail.rc
RUN chown analyticssvc:mail /home/analyticssvc/mail.rc
RUN ln -s /home/analyticssvc/mail.rc /etc/mail.rc
USER analyticssvc
COPY Jobs Jobs/
COPY Tasks Tasks/
COPY ca-bundle.trust.crt /etc/pki/tls/certs/ca-bundle.trust.crt
COPY configure_mailx.sh configure_mailx.sh

CMD [ "sleep","9999999" ]
