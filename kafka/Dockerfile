# A two-line image over apache/kafka, for one reason: volume ownership.
#
# Kafka runs as uid 1000 (appuser) and stores its log at /tmp/kafka-logs (the
# log.dirs in the server.properties this image actually launches with). That
# directory does not exist in the base image, so Docker creates the mount point
# for the named volume as **root** - and Kafka then cannot format its storage:
#
#   Formatting /tmp/kafka-logs ... Error while writing meta.properties file
#
# Docker initializes an *empty* named volume from the image's directory at that
# path, ownership included. Creating it here with the right owner is therefore
# what makes a fresh `docker compose up` work on a machine that has never run
# this stack before.
FROM apache/kafka:3.7.0

USER root
RUN mkdir -p /tmp/kafka-logs && chown -R 1000:1000 /tmp/kafka-logs
USER appuser
