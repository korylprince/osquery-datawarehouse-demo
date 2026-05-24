FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH="$(dpkg --print-architecture)" go build -o /out/data-warehouse-mcp ./cmd/data-warehouse-mcp

FROM debian:bookworm-slim AS session-rootfs
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates coreutils file findutils gawk grep jq less procps \
    python3 python3-pip python3-venv sed tzdata \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --break-system-packages \
    numpy pandas matplotlib seaborn pyarrow scipy scikit-learn statsmodels
RUN mkdir -p /workspace /tmp /out
RUN tar -C / -cf /out/session-rootfs.tar \
    bin etc lib usr workspace tmp $(test -d /lib64 && echo lib64)

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates file tar tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/data-warehouse-mcp /app/data-warehouse-mcp
COPY --from=session-rootfs /out/session-rootfs.tar /opt/session-rootfs.tar
WORKDIR /app
ENTRYPOINT ["/app/data-warehouse-mcp"]
