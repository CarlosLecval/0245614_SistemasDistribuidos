FROM golang:1.23

COPY . /app/

WORKDIR /app

RUN go mod download

CMD ["go", "test", "./..."]
