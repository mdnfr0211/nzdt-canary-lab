# event-gateway — distroless static image
FROM golang:1.23 AS build
WORKDIR /src
COPY main.go go.mod go.sum ./
RUN go mod download && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/gateway"]