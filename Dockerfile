FROM golang:1.23

ENV CONFIG_PATH="/app/.certs"

COPY . /app/

WORKDIR /app

RUN go mod download

CMD ["go", "test", "./..."]
