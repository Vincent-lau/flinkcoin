#!/bin/zsh
docker compose up --build producer -d --remove-orphans
docker ps | grep flinkcoin_producer | awk '{print $1}' | xargs docker logs -f
