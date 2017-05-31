#! /bin/bash

docker service create \
--replicas 1 \
--name ss-saver \
--network swarm-net \
--mount type=bind,src=/mnt/share,dst=/mnt/,readonly \
openjdk:8-jre java -cp /mnt/StreamSaver-0.1.0-SNAPSHOT.jar com.newnius.streamsaver.Main
