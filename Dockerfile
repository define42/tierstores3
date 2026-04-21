FROM --platform=$BUILDPLATFORM golang:1.23.2-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o /out/tierstore ./cmd/tierstore

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /out/tierstore /usr/local/bin/tierstore

WORKDIR /var/lib/tierstore

ENTRYPOINT ["/usr/local/bin/tierstore"]
