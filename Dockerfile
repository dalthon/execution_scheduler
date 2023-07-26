FROM golang:1.20-alpine

WORKDIR /app

RUN apk update                       && \
    apk upgrade                      && \
    apk add --update --no-cache make && \
    rm -rf /var/cache/apk/*
