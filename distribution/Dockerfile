# Debezium Server with the RushDB sink built in.
#
# The official image with its application (runner jar and lib/) replaced by
# this repository's build of the same server plus the RushDB sink; run.sh,
# JMX, metrics and the connectors directory are the official ones.
#
#   mvn install && mvn -f distribution/pom.xml package
#   docker build -f distribution/Dockerfile -t debezium-server-rushdb .
#
# Keep the tag in step with version.debezium in distribution/pom.xml.
FROM quay.io/debezium/server:3.7.1.Final

USER root
RUN rm -rf /debezium/lib/* /debezium/debezium-server-*-runner.jar
COPY --chown=jboss:0 distribution/target/lib/ /debezium/lib/
COPY --chown=jboss:0 distribution/target/debezium-server-rushdb-dist-*-runner.jar /debezium/
USER jboss
