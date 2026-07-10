# مرحله اول: build
FROM docker.abrha.net/golang:1.25-alpine AS builder

# تنظیم mirror داخل Dockerfile
ENV GOPROXY=https://mirror.abrha.net/repository/go/,direct
ENV GOSUMDB=off
ENV GO111MODULE=on

WORKDIR /app
ARG GOPROXY=https://mirror-go.runflare.com
# کپی فایل‌های module
COPY go.mod go.sum ./
RUN GOPROXY=https://mirror-go.runflare.com go mod download
RUN go mod verify

# کپی بقیه کدها
COPY . .

# build
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app ./cmd/

# مرحله دوم: اجرا
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/app .

EXPOSE 8080
CMD ["./app"]