# --- Stage 1: Build ---
FROM rust:1.91-slim-bookworm AS builder
WORKDIR /app
# Copy the entire workspace
COPY . .
# Build only the web app in release mode
RUN cargo build --release --package web

# --- Stage 2: Runtime ---
FROM debian:bookworm-slim
WORKDIR /app

# Install libssl
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder
COPY --from=builder /app/target/release/web /app/web

# Create the data directory
RUN mkdir -p /app/data

# Port Axum is listening on
EXPOSE 3000

# Run the app
CMD ["./web"]
