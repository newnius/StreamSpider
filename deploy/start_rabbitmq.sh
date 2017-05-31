#! /bin/bash

docker service create \
--replicas 1 \
--name ss-rabbitmq \
--hostname ss-rabbitmq \
--network swarm-net \
--publish 15672:15672 \
--user 1000:1000 \
--mount type=bind,source=/mnt/data/ss-rabbitmq,target=/var/lib/rabbitmq \
rabbitmq
