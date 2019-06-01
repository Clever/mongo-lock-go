include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
PKG := github.com/Clever/mongo-lock-go
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE = $(shell basename $(PKG))

.PHONY: test $(PKGS) run clean vendor

$(eval $(call golang-version-check,1.12))

test: $(PKGS)

build:
	go build -o bin/$(EXECUTABLE) $(PKG)

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)


GLIDE_VERSION = v0.12.3
$(GOPATH)/src/github.com/Masterminds/glide:
	git clone -b $(GLIDE_VERSION) https://github.com/Masterminds/glide.git $(GOPATH)/src/github.com/Masterminds/glide

$(GOPATH)/bin/glide: $(GOPATH)/src/github.com/Masterminds/glide
	@go build -o $(GOPATH)/bin/glide github.com/Masterminds/glide



install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)