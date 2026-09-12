#!/usr/bin/env bash

set -euo pipefail

NS=helix
kubectl apply -f deploy/namespace.yaml >/dev/null

cleanup() { kubectl delete pod -n "$NS" ipt-a ipt-b --ignore-not-found --wait=false >/dev/null; }
trap cleanup EXIT

for p in ipt-a ipt-b; do
  kubectl run -n "$NS" "$p" --image=alpine:3.20 --restart=Never \
    --overrides='{"spec":{"containers":[{"name":"'"$p"'","image":"alpine:3.20",
      "command":["sh","-c","apk add -q --no-cache iptables && touch /ready && sleep 3600"],
      "securityContext":{"capabilities":{"add":["NET_ADMIN"]}}}]}}' >/dev/null
done
kubectl wait -n "$NS" --for=condition=Ready pod/ipt-a pod/ipt-b --timeout=120s >/dev/null
for p in ipt-a ipt-b; do
  until kubectl exec -n "$NS" "$p" -- test -f /ready 2>/dev/null; do sleep 1; done
done

A_IP=$(kubectl get pod -n "$NS" ipt-a -o jsonpath='{.status.podIP}')
B_IP=$(kubectl get pod -n "$NS" ipt-b -o jsonpath='{.status.podIP}')
echo "ipt-a=$A_IP ipt-b=$B_IP"

ping_b() { kubectl exec -n "$NS" ipt-b -- ping -c1 -W1 "$A_IP" >/dev/null 2>&1; }

ping_b || { echo "FAIL: pods cannot reach each other before partition"; exit 1; }
echo "ok: b -> a reachable"

kubectl exec -n "$NS" ipt-a -- iptables -I INPUT 1 -s "$B_IP" -j DROP
kubectl exec -n "$NS" ipt-a -- iptables -L INPUT -n
if ping_b; then echo "FAIL: DROP rule did not cut traffic"; exit 1; fi
echo "ok: b -> a blocked by iptables"

kubectl exec -n "$NS" ipt-a -- iptables -F INPUT
ping_b || { echo "FAIL: traffic not restored after heal"; exit 1; }
echo "ok: b -> a restored after flush"
echo "PASS: iptables partitions work inside kind pods"
