FROM golang:1.25.3-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /app/web main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/web /app/web

COPY --from=builder /app/view/public /app/view/public

EXPOSE 8090
EXPOSE 50051

CMD ["./web"]