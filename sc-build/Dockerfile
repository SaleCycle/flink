# NOTES
# This will output the tgz distribution and the kinesis connector to whichever local folder you associate with the
# exported volume /out/dist
#
# This has been tested with docker containers set to 6gb memory and 2 CPUs
#
# Example:
# docker build -t flink-build .
# docker run -v /path/to/local/folder:/out/dist flink-build

FROM openjdk:8u162

RUN git clone --single-branch --branch release-tag-1.9.0 https://github.com/SaleCycle/flink.git

RUN curl --silent --show-error --location --fail --retry 3 --output /tmp/apache-maven.tar.gz https://www.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz && \
  tar xf /tmp/apache-maven.tar.gz -C /opt/ && \
  rm /tmp/apache-maven.tar.gz && \
  ln -s /opt/apache-maven-* /opt/apache-maven && \
  /opt/apache-maven/bin/mvn -version

COPY run.sh .

ENV MAVEN_OPTS="-Xmx5500M -XX:MetaspaceSize=3000M -XX:MaxMetaspaceSize=5500M -XX:+CMSClassUnloadingEnabled"
VOLUME /out/dist
WORKDIR flink

CMD ["../run.sh"]
