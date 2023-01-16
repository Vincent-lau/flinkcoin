#!/bin/zsh
docker compose up --build producer taskmanager -d --remove-orphans && \
mvn package && flink run target/flinkcoin-0.1.jar && \
docker ps --no-trunc | grep task | awk '{print $1}' | xargs docker logs -f
