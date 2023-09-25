FROM golang:1.21-alpine

WORKDIR /app

RUN apk update                       && \
    apk upgrade                      && \
    apk add --update --no-cache make && \
    rm -rf /var/cache/apk/*
