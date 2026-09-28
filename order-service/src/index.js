import express from "express";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { explain, partitionFor } from "./partitioner.js";
import { renderLayout } from "./layout.js";

const TOPIC = "orders";
const PARTITIONS = Number(process.env.PARTITION_COUNT ?? 3);
const brokers = [process.env.KAFKA_BROKER ?? "localhost:9092"];

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers, logLevel: KafkaJS.logLevel.NOTHING },
});

// ⚠️ We set NO partitioner option here — this is the stock default.
// In this client the default is murmur2_random, the same algorithm the
// official Java client uses. partitioner.js reimplements it so we can
// predict the outcome; Kafka itself is doing the real routing.
const producer = kafka.producer({
  kafkaJS: {
    // Retries can never reorder or duplicate a message. Per-key ordering is only
    // guaranteed with this ON — librdkafka ships it OFF, modern Java clients ON.
    idempotent: true,
  },
});
await producer.connect();

const admin = kafka.admin();
await admin.connect();

/** Everything we've sent, with the partition the BROKER acked it to. */
const ledger = [];
/** Per-customer counter, so ordering within a partition is visible. */
const seq = new Map();

/**
 * Send one order.
 *
 * @param customerId  used as the Kafka message key — this is the whole point
 * @param keyed       false → send with NO key, to show what breaks
 */
async function placeOrder({ customerId, item, amount, keyed = true }) {
  const n = (seq.get(customerId) ?? 0) + 1;
  seq.set(customerId, n);

  const order = { id: crypto.randomUUID(), customerId, seq: n, item, amount };

  const [ack] = await producer.send({
    topic: TOPIC,
    messages: [
      {
        // 🔑 THE ONE LINE THIS ENTIRE REPO IS ABOUT.
        //    A key means "these messages belong together, keep them in order".
        //    No key means "I don't care, spread them for throughput".
        key: keyed ? customerId : null,
        value: JSON.stringify(order),
      },
    ],
  });

  const entry = {
    ...order,
    keyed,
    partition: ack.partition, // ← the broker's answer, not our guess
    offset: ack.baseOffset,
    predicted: keyed ? partitionFor(customerId, PARTITIONS) : null,
  };
  ledger.push(entry);

  console.log(
    `📤 ${keyed ? `key=${customerId}` : "key=<none>"} `.padEnd(22) +
      `→ partition ${entry.partition}  offset ${entry.offset}  ${item}`
  );
  return entry;
}

/** The scripted burst the reel is built around. */
const SCRIPT = [
  { customerId: "alice", item: "Margherita Pizza", amount: 499 },
  { customerId: "sam", item: "Sushi Platter", amount: 899 },
  { customerId: "leo", item: "Beef Tacos", amount: 349 },
  { customerId: "alice", item: "Garlic Bread", amount: 149 },
  { customerId: "maria", item: "Cheeseburger", amount: 399 },
  { customerId: "sam", item: "Miso Ramen", amount: 549 },
  { customerId: "alice", item: "Tiramisu", amount: 249 },
  { customerId: "leo", item: "Spring Rolls", amount: 199 },
  { customerId: "sam", item: "Green Tea", amount: 99 },
];

const app = express();
app.use(express.json());

/** Place a single order. Keyed by customerId. */
app.post("/api/order", async (req, res) => {
  const { customerId, item, amount } = req.body ?? {};
  if (!customerId) return res.status(400).json({ error: "customerId is required — it's the Kafka key" });
  res.json(await placeOrder({ customerId, item, amount }));
});

/** The same order, sent WITHOUT a key — the counter-example. */
app.post("/api/order/unkeyed", async (req, res) => {
  const { customerId, item, amount } = req.body ?? {};
  res.json(await placeOrder({ customerId: customerId ?? "anonymous", item, amount, keyed: false }));
});

/** Fire the scripted burst. ?keyed=false sends the same 9 orders with no key. */
app.post("/api/demo", async (req, res) => {
  const keyed = req.query.keyed !== "false";
  console.log(`\n── sending 9 orders ${keyed ? "WITH keys" : "WITHOUT keys"} ─────────────`);
  for (const order of SCRIPT) await placeOrder({ ...order, keyed });
  console.log("──────────────────────────────────────────────\n");
  res.type("text/plain").send(renderLayout(ledger, PARTITIONS));
});

/** Show the maths for a key without sending anything. */
app.get("/api/where/:key", (req, res) => {
  res.json(explain(req.params.key, PARTITIONS));
});

/** The proof table: what actually landed where, per the broker. */
app.get("/api/layout", (req, res) => {
  res.type("text/plain").send(renderLayout(ledger, PARTITIONS));
});

app.get("/api/layout.json", (_, res) => res.json(ledger));

/** Reset between takes: clear the ledger and empty the topic. */
app.delete("/api/demo", async (_, res) => {
  ledger.length = 0;
  seq.clear();
  const offsets = await admin.fetchTopicOffsets(TOPIC);
  await admin.deleteTopicRecords({
    topic: TOPIC,
    partitions: offsets.map(({ partition }) => ({ partition, offset: "-1" })),
  });
  console.log("🧹 ledger cleared + topic emptied");
  res.json({ status: "CLEARED" });
});

app.listen(3000, () =>
  console.log(`🟢 order-service on :3000  ·  topic "${TOPIC}" with ${PARTITIONS} partitions`)
);

// Flush anything still in flight before the container stops.
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    await producer.disconnect();
    await admin.disconnect();
    process.exit(0);
  });
}
