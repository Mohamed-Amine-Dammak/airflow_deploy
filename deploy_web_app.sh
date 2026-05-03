#!/bin/bash
set -e

NEW_TAG="$1"

if [ -z "$NEW_TAG" ]; then
  echo "Usage: ./deploy_web_app.sh <image-tag>"
  exit 1
fi

CURRENT_TAG=$(grep WEB_APP_IMAGE_TAG .env | cut -d '=' -f2 || true)

if [ -n "$CURRENT_TAG" ]; then
  echo "$CURRENT_TAG" > previous_web_app_version.txt
fi

sed -i "s/^WEB_APP_IMAGE_TAG=.*/WEB_APP_IMAGE_TAG=$NEW_TAG/" .env

docker compose pull web_app
docker compose up -d web_app

echo "$NEW_TAG" > current_web_app_version.txt