FROM golang:1.21-alpine

WORKDIR /app

# Install dependencies
# Note: we copy go.mod and go.sum first to leverage Docker cache
COPY go.mod ./
# RUN go mod download

COPY . .

RUN go build -o /luminadb ./cmd/luminadb
RUN go build -o /lumina-cli ./cmd/lumina-cli

EXPOSE 50051

ENTRYPOINT ["/luminadb"]
