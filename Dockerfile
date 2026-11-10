FROM node:26-bookworm-slim AS web
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.27.3-bookworm AS server
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY --from=web /src/internal/relay/public ./internal/relay/public
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /relay ./cmd/relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=server /relay /relay
ENV RELAY_LISTEN=:8080
EXPOSE 8080
ENTRYPOINT ["/relay"]
