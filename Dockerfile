# Multi-stage build for cc-live-relay (.NET 10).
# The wire protocol, endpoints and env contract are fixed, so this image is a drop-in
# replacement behind GO's Relay.base_url.

# ── Build stage ─────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/CcLiveRelay/CcLiveRelay.csproj -c Release -o /app/publish

# ── Runtime stage ───────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

# curl is used by the HEALTHCHECK (the base image ships without it).
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# The relay binds HOST:PORT itself.
ENV HOST=0.0.0.0
ENV PORT=8765
# The aspnet image defaults HTTP_PORTS=8080; the relay binds its own URL, so neutralise it.
ENV ASPNETCORE_HTTP_PORTS=""

# Replays are written to ./replays relative to the working directory.
RUN mkdir -p /app/replays

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8765/health || exit 1

ENTRYPOINT ["dotnet", "CcLiveRelay.dll"]
