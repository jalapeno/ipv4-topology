REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all ipv4-topology container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: ipv4-topology

ipv4-topology:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-ipv4-topology

ipv4-topology-container: ipv4-topology
	docker build -t $(REGISTRY_NAME)/ipv4-topology:$(IMAGE_VERSION) -f ./build/Dockerfile.ipv4-topology .

push: ipv4-topology-container
	docker push $(REGISTRY_NAME)/ipv4-topology:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
