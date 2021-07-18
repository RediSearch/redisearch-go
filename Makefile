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

.PHONY: all test coverage get checkfmt fmt godoc \
	examples godoc_examples start-redis stop-redis monitor

all: test coverage examples

get:
	$(GOGET) -t -v ./redisearch/...

TLS_CERT ?= redis.crt
TLS_KEY ?= redis.key
TLS_CACERT ?= ca.crt

REDIS_SERVER ?= localhost:6379
export REDISEARCH_TEST_HOST=$(REDIS_SERVER)

REDIS_HOST:=$(word 1,$(subst :, ,$(REDIS_SERVER)))
REDIS_PORT:=$(word 2,$(subst :, ,$(REDIS_SERVER)))
ifeq ($(word 1,$(REDIS_HOST)),)
REDIS_HOST:=localhost
endif
ifeq ($(word 1,$(REDIS_PORT)),)
REDIS_PORT:=6379
endif

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
						 --host $(REDIS_SERVER)

fmt:
	$(GOFMT) ./...

godoc_examples: get fmt
	$(GOTEST) -race -covermode=atomic ./redisearch

TEST ?= Test
ifeq ($(VERBOSE),1)
TEST_FLAGS += -v
endif

test: get fmt
	$(GOTEST) $(TEST_FLAGS) -run $(TEST) ./redisearch

coverage: get
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./redisearch

godoc:
	$(GOGET) -u golang.org/x/tools/...
	echo "Open browser tab on localhost:6060"
	$(GODOC)

start-redis:
	@docker run --name redisearch-go-tests -d --rm -p 6379:6379 redislabs/redisearch:edge

stop-redis:
	@docker stop redisearch-go-tests

monitor:
	@redis-cli -h $(REDIS_HOST) -p $(REDIS_PORT) monitor
