CLUSTER       := helix
NS            := helix
PY            := .venv/bin/python
SEED          ?= 42
KIND          ?= $(shell command -v kind 2>/dev/null || echo $(HOME)/go/bin/kind)
KV_IMAGE      := helix/kvnode:dev
CONTROL_IMAGE := helix/control:dev

.PHONY: build run-node images kind-up kind-down deploy undeploy smoke-iptables smoke-raft test \
        run-baseline run-naive run-fixed check viz

build:
	go build -o bin/kvnode ./cmd/kvnode

run-node: build
	rm -rf bin/data/kv-0
	./bin/kvnode --id kv-0 --client-addr 127.0.0.1:8000 --raft-addr 127.0.0.1:7000 --data-dir bin/data/kv-0 --read-mode local

images:
	CGO_ENABLED=0 GOOS=linux go build -o bin/kvnode-linux ./cmd/kvnode
	docker build -q -f deploy/docker/kvnode.Dockerfile -t $(KV_IMAGE) .
	docker build -q -f deploy/docker/control.Dockerfile -t $(CONTROL_IMAGE) .
	$(KIND) load docker-image --name $(CLUSTER) $(KV_IMAGE) $(CONTROL_IMAGE)

kind-up:
	$(KIND) get clusters | grep -qx $(CLUSTER) || $(KIND) create cluster --config deploy/kind-config.yaml
	kubectl apply -f deploy/namespace.yaml

kind-down:
	$(KIND) delete cluster --name $(CLUSTER)

deploy:
	kubectl apply -f deploy/namespace.yaml -f deploy/kv-headless-svc.yaml -f deploy/kv-statefulset.yaml
	kubectl -n $(NS) delete pod control --ignore-not-found
	kubectl apply -f deploy/control-pod.yaml
	kubectl -n $(NS) rollout restart statefulset/kv
	kubectl -n $(NS) rollout status statefulset/kv --timeout=180s
	kubectl -n $(NS) wait --for=condition=Ready pod/control --timeout=120s

undeploy:
	kubectl -n $(NS) delete statefulset kv --ignore-not-found --wait
	kubectl -n $(NS) delete pod control --ignore-not-found
	kubectl -n $(NS) delete pvc -l app=kv --ignore-not-found
	kubectl -n $(NS) delete svc kv --ignore-not-found

smoke-iptables:
	scripts/smoke_iptables.sh

smoke-raft:
	$(PY) -m harness.smoke_raft

test:
	go vet ./...
	go test -race ./...
	$(PY) -m pytest harness/tests -q

run-baseline run-naive run-fixed check viz:
	@echo "TODO: $@ (Parts 3-8)"
