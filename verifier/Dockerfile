# ── verifier · proof harness ─────────────────────────────────────────────────
# Sends real messages to the real broker and checks the partitioning claims
# in the README. Run with:  docker compose run --rm verifier
FROM node:24-alpine

WORKDIR /app
ENV NODE_ENV=production

# Manifests first, so `npm ci` stays cached until dependencies change.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src

# Don't run as root.
USER node

CMD ["node", "src/index.js"]
