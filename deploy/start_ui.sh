#! /bin/bash

docker service create \
--replicas 1 \
--name ss-ui \
--network swarm-net \
--publish 8086:80 \
--mount type=bind,source=/mnt/data/apache/StreamSpiderUI,target=/var/www/html \
newnius/php:5.6
