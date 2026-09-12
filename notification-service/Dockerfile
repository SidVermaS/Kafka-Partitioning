# ── notification-service · a CONSUMER ────────────────────────────────────────
# One image, three containers (consumer-1, -2, -3). They share a groupId, so
# Kafka splits the topic's partitions between them — one partition, one owner.
FROM node:24-alpine

WORKDIR /app
ENV NODE_ENV=production

# Manifests first, so `npm ci` stays cached until dependencies change.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src

# Don't run as root.
USER node

# Exec form, so node is PID 1 and receives SIGTERM directly. On
# `docker compose stop` the consumer leaves its group immediately and its
# partitions move to the others in ~2s, instead of waiting out the 45s
# session timeout.
CMD ["node", "src/index.js"]
