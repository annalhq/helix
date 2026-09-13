#!/bin/sh
# kvnode must not be PID 1: signals such as SIGSTOP from kubectl exec would be ignored

child=
trap 'kill -TERM "$child" 2>/dev/null; wait "$child"; exit 0' TERM INT

while true; do
  kvnode "$@" &
  child=$!
  wait "$child"
  echo "supervise: kvnode exited with status $?, restarting in 1s" >&2
  sleep 1
done
