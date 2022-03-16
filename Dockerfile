FROM centos/python-38-centos7:latest

LABEL maintainer Sebastian Wozniewski <sebastian.wozniewski@cern.ch>

WORKDIR /home/analyticssvc

USER root
RUN yum update -y
RUN yum -y --enablerepo=extras install epel-release
RUN yum -y install ssmtp
RUN pip install cx_Oracle
RUN pip install elasticsearch
COPY ca-bundle.trust.crt /etc/pki/tls/certs/ca-bundle.trust.crt
COPY ssmtp /etc/ssmtp/
RUN chown root:mail /etc/ssmtp/ssmtp.conf
RUN chown root:mail /etc/ssmtp/revaliases
COPY configure_ssmtp.sh configure_ssmtp.sh
COPY Jobs Jobs/
COPY Tasks Tasks/

CMD [ "sleep","9999999" ]
