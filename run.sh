#!/bin/bash

JARFILES=""
for f in ./target/dependency/*.jar; do
  JARFILES=${JARFILES}":"$f
done

java -cp $JARFILES:./target/kafka-0.0.1-SNAPSHOT.jar myxof.git.kafka.App