/**
 * ───────────────────────────────────────────────────────────────────
 *  Proof that the partitioning in this repo is the real thing.
 * ───────────────────────────────────────────────────────────────────
 *
 *  Run with:  docker compose run --rm verifier
 *
 *  This talks to the actual Kafka broker. It does not mock anything.
 *  It creates a scratch topic, sends real messages, reads them back,
 *  and checks the claims the README makes.
 */
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { murmur2, partitionFor, REFERENCE_VECTORS } from "./partitioner.js";

const brokers = [process.env.KAFKA_BROKER ?? "localhost:9092"];
const TOPIC = `verify-${Date.now()}`;
const PARTITIONS = 3;
const CUSTOMERS = ["alice", "priya", "chen", "maria", "omar", "yuki"];
const ORDERS_EACH = 5;

let failed = 0;
const check = (name, ok, detail = "") => {
  if (!ok) failed++;
  console.log(`  ${ok ? "✅" : "❌"} ${name}${detail ? `\n       ${detail}` : ""}`);
};
const section = (t) => console.log(`\n\x1b[1m${t}\x1b[0m\n`);

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers, logLevel: KafkaJS.logLevel.NOTHING } });
const admin = kafka.admin();
await admin.connect();

try {
  // ═══════════════════════════════════════════════════════════════════
  section("1 · The hash — no broker needed");

  const vectorsOk = REFERENCE_VECTORS.every(([k, want]) => murmur2(Buffer.from(k, "utf8")) === want >>> 0);
  check(
    `murmur2 matches all ${REFERENCE_VECTORS.length} librdkafka reference vectors`,
    vectorsOk,
    "source: librdkafka src/rdmurmur2.c → java_murmur2_results[]"
  );

  // ═══════════════════════════════════════════════════════════════════
  section("2 · The broker agrees with partitioner.js");

  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }],
  });

  const producer = kafka.producer({ kafkaJS: { idempotent: true } });
  await producer.connect();

  const sent = [];
  for (let round = 1; round <= ORDERS_EACH; round++) {
    for (const customerId of CUSTOMERS) {
      const [ack] = await producer.send({
        topic: TOPIC,
        messages: [{ key: customerId, value: JSON.stringify({ customerId, seq: round }) }],
      });
      sent.push({
        customerId,
        seq: round,
        actual: ack.partition,
        predicted: partitionFor(customerId, PARTITIONS),
        offset: Number(ack.baseOffset),
      });
    }
  }

  const mismatches = sent.filter((m) => m.predicted !== m.actual);
  check(
    `predicted partition === broker-acked partition for all ${sent.length} messages`,
    mismatches.length === 0,
    mismatches.length
      ? `first mismatch: key=${mismatches[0].customerId} predicted p${mismatches[0].predicted} got p${mismatches[0].actual}`
      : CUSTOMERS.map((c) => `${c}→p${partitionFor(c, PARTITIONS)}`).join("  ")
  );

  // ═══════════════════════════════════════════════════════════════════
  section("3 · Same key always lands on the same partition");

  for (const c of CUSTOMERS) {
    const parts = new Set(sent.filter((m) => m.customerId === c).map((m) => m.actual));
    check(`"${c}" — ${ORDERS_EACH} messages, ${parts.size} distinct partition`, parts.size === 1);
  }

  // ═══════════════════════════════════════════════════════════════════
  section("4 · Ordering survives the round trip");

  const consumer = kafka.consumer({
    kafkaJS: { groupId: `verify-${Date.now()}`, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topics: [TOPIC] });

  const received = [];
  await new Promise(async (resolve) => {
    const done = setTimeout(resolve, 15000);
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        received.push({ partition, offset: Number(message.offset), ...JSON.parse(message.value) });
        if (received.length >= sent.length) {
          clearTimeout(done);
          resolve();
        }
      },
    });
  });

  check(`read back all ${sent.length} messages`, received.length === sent.length, received.length === sent.length ? "" : `got ${received.length}`);

  for (const c of CUSTOMERS) {
    const mine = received.filter((m) => m.customerId === c).sort((a, b) => a.offset - b.offset);
    const inOrder = mine.every((m, i) => m.seq === i + 1);
    check(`"${c}" — offsets ascending ⇒ seq 1..${ORDERS_EACH} in order`, inOrder, inOrder ? "" : mine.map((m) => m.seq).join(","));
  }

  // ═══════════════════════════════════════════════════════════════════
  section("5 · No key ⇒ no ordering guarantee");

  const noKeyPartitions = new Set();
  for (let i = 0; i < 30; i++) {
    const [ack] = await producer.send({
      topic: TOPIC,
      messages: [{ key: null, value: JSON.stringify({ customerId: "anonymous", seq: i }) }],
    });
    noKeyPartitions.add(ack.partition);
  }
  check(
    `30 keyless messages scattered across ${noKeyPartitions.size} partitions (keyed ones never scatter)`,
    noKeyPartitions.size > 1,
    `partitions used: ${[...noKeyPartitions].sort().join(", ")}`
  );

  // ═══════════════════════════════════════════════════════════════════
  section("6 · Changing the partition count remaps keys (the one-way door)");

  const remapped = CUSTOMERS.filter((c) => partitionFor(c, 3) !== partitionFor(c, 4));
  check(
    `${remapped.length}/${CUSTOMERS.length} keys move partition when 3 → 4`,
    remapped.length > 0,
    CUSTOMERS.map((c) => `${c}: p${partitionFor(c, 3)}→p${partitionFor(c, 4)}`).join("  ")
  );

  await consumer.disconnect();
  await producer.disconnect();
} finally {
  await admin.deleteTopics({ topics: [TOPIC] }).catch(() => {});
  await admin.disconnect();
}

console.log(
  failed === 0
    ? "\n\x1b[32m  ALL CHECKS PASSED — verified against a live Kafka broker.\x1b[0m\n"
    : `\n\x1b[31m  ${failed} CHECK(S) FAILED\x1b[0m\n`
);
process.exit(failed === 0 ? 0 : 1);
