CLUSTER := helix
NS      := helix
PY      := .venv/bin/python
SEED    ?= 42
KIND    ?= $(shell command -v kind 2>/dev/null || echo $(HOME)/go/bin/kind)

.PHONY: build run-node images kind-up kind-down deploy smoke-iptables test \
        run-baseline run-naive run-fixed check viz

build:
	go build -o bin/kvnode ./cmd/kvnode

run-node: build
	./bin/kvnode --id kv-0 --client-addr 127.0.0.1:8000 --read-mode local

images:
	@echo "TODO (Part 2c): docker build + kind load"

kind-up:
	$(KIND) get clusters | grep -qx $(CLUSTER) || $(KIND) create cluster --config deploy/kind-config.yaml
	kubectl apply -f deploy/namespace.yaml

kind-down:
	$(KIND) delete cluster --name $(CLUSTER)

deploy:
	@echo "TODO (Part 2c): kubectl apply StatefulSet + control pod"

smoke-iptables:
	scripts/smoke_iptables.sh

test:
	go vet ./...
	go test -race ./...
	$(PY) -m pytest harness/tests -q

run-baseline run-naive run-fixed check viz:
	@echo "TODO: $@ (Parts 3-8)"
