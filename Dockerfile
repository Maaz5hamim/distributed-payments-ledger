FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /bin/ledger-api ./cmd/api && \
 CGO_ENABLED=0 go build -o /bin/ledger-worker ./cmd/worker && \
 CGO_ENABLED=0 go build -o /bin/ledger-relay ./cmd/relay && \
 CGO_ENABLED=0 go build -o /bin/ledger-reconciler ./cmd/reconciler && \
 CGO_ENABLED=0 go build -o /bin/ledger-migrate ./cmd/migrate && \
 CGO_ENABLED=0 go build -o /bin/ledger-consumer ./cmd/consumer
FROM alpine:3.21
RUN adduser -D ledger
WORKDIR /app
COPY --from=build /bin/ledger-* /usr/local/bin/
COPY db ./db
USER ledger
EXPOSE 8080 9090
CMD ["ledger-api"]
