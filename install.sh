#!/bin/sh

if [[ -z "${NIFI_HOME}" ]]
then
  echo "Please set your NIFI_HOME environment variable (try `nifi status`)"
  exit 2
fi

if [[ ! -d "${NIFI_HOME}/lib" ]]
then
  echo "Your NIFI_HOME (${NIFI_HOME}/lib) lib directory is not found"
  exit 3
fi

echo "Installing:"
if compgen -G "nuxeo-nifi-services-nar/target/*.nar" && compgen -G "nuxeo-nifi-api-nar/target/*.nar"
then
  echo "Copy..."
  cp -vf nuxeo-nifi-*-nar/target/*.nar "${NIFI_HOME}/lib"
  echo "Done."
else
  echo "Please build project before attempting install `mvn clean install`"
  exit 1
fi
