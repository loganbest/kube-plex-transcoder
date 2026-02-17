FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o kube-plex .

FROM alpine:3.21
COPY --from=builder /app/kube-plex /kube-plex
ENTRYPOINT ["/kube-plex"]
