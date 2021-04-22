# Go parameters
GOCMD=GO111MODULE=on go

GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test -count=1
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GODOC=godoc

.PHONY: all test coverage
all: test coverage examples

get:
	$(GOGET) -t -v ./redisearch/...

TLS_CERT ?= redis.crt
TLS_KEY ?= redis.key
TLS_CACERT ?= ca.crt
REDISEARCH_TEST_HOST ?= 127.0.0.1:6379

checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

examples: get
	@echo " "
	@echo "Building the examples..."
	$(GOBUILD) ./examples/redisearch_quickstart/.
	$(GOBUILD) ./examples/redisearch_auth/.
	$(GOBUILD) ./examples/redisearch_geo/.
	$(GOBUILD) ./examples/redisearch_tls_client/.
	$(GOBUILD) ./examples/redisearch_temporary_index/.
	./redisearch_tls_client --tls-cert-file $(TLS_CERT) \
						 --tls-key-file $(TLS_KEY) \
						 --tls-ca-cert-file $(TLS_CACERT) \
						 --host $(REDISEARCH_TEST_HOST)

fmt:
	$(GOFMT) ./...

godoc_examples: get fmt
	$(GOTEST) -race -covermode=atomic ./redisearch

test: get fmt
	$(GOTEST) -run "Test" ./redisearch

coverage: get
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./redisearch

godoc:
	$(GOGET) -u golang.org/x/tools/...
	echo "Open browser tab on localhost:6060"
	$(GODOC)

