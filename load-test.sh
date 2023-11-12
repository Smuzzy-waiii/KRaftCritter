#!/bin/bash

BASE_URL="http://localhost:40052/setValue"
count="${1:-0}"  # Use the first command-line argument as the number of iterations, defaulting to 0 for infinite
iteration=0

while [ "$count" -eq 0 ] || [ "$iteration" -lt "$count" ]; do
  current_time=$(gdate +"%H:%M:%S.%3N")
  URL="$BASE_URL?key=hi&value=$current_time"
  response=$(curl -s -X POST "$URL")
  echo "HTTP POST request sent to URL: $URL"
  iteration=$((iteration + 1))
done

