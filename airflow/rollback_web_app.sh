#!/bin/bash
set -e

PREVIOUS_TAG=$(cat previous_web_app_version.txt)

./deploy_web_app.sh "$PREVIOUS_TAG"