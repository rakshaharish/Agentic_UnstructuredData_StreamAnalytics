#!/bin/bash
set -e

ollama serve &
OLLAMA_SERVE_PID=$!

until ollama list >/dev/null 2>&1; do
  sleep 1
done

ollama pull llama3

wait $OLLAMA_SERVE_PID
