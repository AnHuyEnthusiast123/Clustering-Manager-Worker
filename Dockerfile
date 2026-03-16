# Build stage
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY task_config.json ./
RUN CGO_ENABLED=0 GOOS=linux go build -o main main.go

# Run stage
FROM alpine:latest
WORKDIR /app

# Copy binary và config
COPY --from=builder /app/main .
COPY --from=builder /app/task_config.json .

# QUAN TRỌNG: Đảm bảo binary có quyền execute
RUN chmod +x ./main

# Thêm certs (nếu dùng HTTPS sau này)
RUN apk add --no-cache ca-certificates tzdata

EXPOSE 2112
CMD ["./main"]