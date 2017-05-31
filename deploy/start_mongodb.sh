#! /bin/bash

docker service create \
--replicas 1 \
--name ss-mongo \
--network swarm-net \
--mount type=bind,source=/home/test/mongo,target=/data/db \
--constraint node.hostname==ubuntu-5 \
--endpoint-mode dnsrr \
mongo
