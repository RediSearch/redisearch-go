# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

.PHONY: all test coverage
all: test coverage examples

get:
	$(GOGET) -t -v ./redisearch/...

TLS_CERT ?= redis.crt
TLS_KEY ?= redis.key
TLS_CACERT ?= ca.crt
REDISEARCH_TEST_HOST ?= 127.0.0.1:6379

examples: get
	@echo " "
	@echo "Building the examples..."
	$(GOBUILD) ./examples/redisearch_quickstart/.
	$(GOBUILD) ./examples/redisearch_auth/.
	$(GOBUILD) ./examples/redisearch_tls_client/.
	$(GOBUILD) ./examples/redisearch_temporary_index/.
	./redisearch_tls_client --tls-cert-file $(TLS_CERT) \
						 --tls-key-file $(TLS_KEY) \
						 --tls-ca-cert-file $(TLS_CACERT) \
						 --host $(REDISEARCH_TEST_HOST)

test: get
	$(GOTEST) -race -covermode=atomic ./...

coverage: get test
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./redisearch

