FROM golang:1.20 as build

WORKDIR /app
COPY go.mod .
COPY go.sum .
#Adding changed files last for hitting docker layer cache
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/bee-copy

# Switch to a small base image
FROM scratch

# Get the TLS CA certificates from the build container, they're not provided by busybox.
COPY --from=build /etc/ssl/certs /etc/ssl/certs

# copy app to bin directory, and set it as entrypoint
WORKDIR /app
COPY --from=build /app/bee-copy /app/bee-copy
COPY --from=build /app/resources/ /app/resources

EXPOSE 8082

ENTRYPOINT ["/app/bee-copy"]