#! /bin/bash


/opt/apache-maven/bin/mvn clean package -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Dhadoop.version=2.6.5 -Pinclude-kinesis

cp flink-connectors/flink-connector-kinesis/target/flink-connector-kinesis*.jar /out/dist
cp -R flink-dist/target/flink-*-bin /out/dist

cd /out/dist
tar cvfz ./flink-dist.tgz flink-*-bin/flink-*/**/*

rm -rf flink-*-bin
rm *-tests.jar
