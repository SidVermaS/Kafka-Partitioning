# ── order-service · the PRODUCER ─────────────────────────────────────────────
# Decides nothing about partitions itself: it attaches a key (customerId) to
# every message, and the Kafka client turns that key into a partition number.
FROM node:24-alpine

WORKDIR /app
ENV NODE_ENV=production

# Manifests first, so `npm ci` stays cached until dependencies change.
# @confluentinc/kafka-javascript bundles librdkafka — the C library that runs
# the murmur2 partitioner on every send().
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src ./src

# Don't run as root.
USER node

EXPOSE 3000

# Exec form, so node is PID 1 and receives SIGTERM directly — the shutdown
# handler in src/index.js flushes in-flight messages before exiting.
CMD ["node", "src/index.js"]
