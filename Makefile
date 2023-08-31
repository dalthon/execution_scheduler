SH ?= ash

IMAGE_NAME := execution-scheduler

DOCKER_WORKDIR ?= `pwd`
DOCKER_RUN := docker run --rm -v $(DOCKER_WORKDIR):/app -it
DOCKER_BIN := $(shell which docker)

define docker_run
	if [ -n "$(DOCKER_BIN)" ]; then echo "Running on $1 container..." && $(DOCKER_RUN) $1 $2; else $2; fi;
endef

default: help ## Defaults to help

help: ## Get help
	@echo "Make tasks:\n"
	@grep -hE '^[%a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-10s\033[0m %s\n", $$1, $$2}'
	@echo ""
.PHONY: help

build :## Builds docker image
	docker build -t $(IMAGE_NAME) .
.PHONY: build

shell: ## Runs shell inside container
	@$(call docker_run,$(IMAGE_NAME),$(SH))
.PHONY: shell

test: ## Runs tests
	@$(call docker_run,$(IMAGE_NAME),go test)
.PHONY: test

test-%: ## Runs specific test
	@$(call docker_run,$(IMAGE_NAME),go test -run $*)
.PHONY: test-%

debug: ## Runs debug/main.go
	@$(call docker_run,$(IMAGE_NAME),go run debug/main.go)
.PHONY: debug
