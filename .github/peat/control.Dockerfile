# The control image: Peat's control layer, gateways and operator, one TypeScript program run by Bun (control/src/main.ts names the role:
# operator, control or gateway). It needs nothing but Bun: the program imports only Node's and Bun's own modules. Built from the
# control folder: docker build -f .github/peat/control.Dockerfile control
FROM oven/bun:1.3.14-slim@sha256:d56a2534ffd262e92c12fd3249d3924d296d97086da773f821d7d0477435ea04
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/Zabaca/peat" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.description="Peat's control layer, gateways and operator"
WORKDIR /app
COPY package.json tsconfig.json ./
COPY src ./src
USER 1000:1000
ENTRYPOINT ["bun", "/app/src/main.ts"]
