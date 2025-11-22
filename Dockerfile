FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o autoscaler .

FROM alpine:3.20
WORKDIR /root
COPY --from=builder /app/autoscaler /usr/local/bin/autoscaler
ENV HTTP_PORT=8088
CMD ["/usr/local/bin/autoscaler"]
