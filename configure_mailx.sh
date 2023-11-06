#!/bin/bash

echo "set smtp=smtp://${ESA_SMTP}" >> mail.rc
echo "set from=${ESA_EMAIL}" >> mail.rc
#echo "set smtp-auth=login" >> mail.rc
#echo "set smtp-auth-user=${ESA_USER}" >> mail.rc
#echo "set smtp-auth-password=${ESA_PASS}" >> mail.rc
#echo "set smtp-use-starttls" >> mail.rc
