#!/bin/bash

# Set up variables from Environment
GITHUB_USER="${GITHUB_USER}"
GITHUB_TOKEN="${TOKEN}"
IMDB_REPO="${REPO}"
IMDB_REPO_PATH="${IMDB_REPO_PATH}"

# Get the latest workflow run
response=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/repos/$GITHUB_USER/$IMDB_REPO/actions/runs?per_page=1")

# Check if the last run was successful
status=$(echo $response | jq -r '.workflow_runs[0].conclusion')

if [ "$status" == "success" ]; then
    echo "The last GitHub Actions run was successful."
    cd $IMDB_REPO_PATH
    git pull origin develop
    docker compose down
    docker compose up --build
else
    echo "The last GitHub Actions run was not successful. Status: $status"
fi