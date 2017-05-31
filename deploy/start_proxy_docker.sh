#! /bin/bash

docker service create \
--replicas 1 \
--name ss-proxy \
--network swarm-net \
newnius/docker-proxy
