# Dockerfile

ARG FROM_IMAGE=gcr.io/abilitec-common-gcr/abilitec-graph-hadoop-common-develop:latest

FROM $FROM_IMAGE

WORKDIR /usr/src/source_scorecard

COPY mapper1.py .
COPY reducer1.py .
COPY run_source_scorecard.py .
COPY source_scorecard_standalone.py .
