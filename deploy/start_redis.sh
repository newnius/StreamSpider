#! /bin/bash

docker service create \
--replicas 1 \
--name ss-redis \
--network swarm-net \
--endpoint-mode dnsrr \
--user 1000:1000 \
--mount type=bind,source=/mnt/data/ss-redis,target=/data \
redis redis-server --appendonly yes
