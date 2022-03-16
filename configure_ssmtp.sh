#!/bin/bash

sed -i "s/ESA_USER/${ESA_USER}/g" /etc/ssmtp/ssmtp.conf
sed -i "s/ESA_PASS/${ESA_PASS}/g" /etc/ssmtp/ssmtp.conf
sed -i "s/ESA_EMAIL/${ESA_EMAIL}/g" /etc/ssmtp/ssmtp.conf
sed -i "s/ESA_EMAIL/${ESA_EMAIL}/g" /etc/ssmtp/revaliases
