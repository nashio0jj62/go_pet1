FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go mod tidy
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api
RUN CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -trimpath -o /out/projection ./cmd/projection
RUN CGO_ENABLED=0 go build -trimpath -o /out/mock-psp ./cmd/mock-psp

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/worker /app/worker
COPY --from=build /out/projection /app/projection
COPY --from=build /out/mock-psp /app/mock-psp
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/api"]

