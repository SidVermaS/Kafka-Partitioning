import { KafkaJS } from "@confluentinc/kafka-javascript";

const TOPIC = "orders";
const NAME = process.env.CONSUMER_NAME ?? "consumer";
const GROUP = process.env.GROUP_ID ?? "notification-service";
const brokers = [process.env.KAFKA_BROKER ?? "localhost:9092"];

/** Colour by PARTITION, not by consumer — so partition grouping pops in the logs. */
const COLOURS = ["\x1b[32m", "\x1b[33m", "\x1b[35m", "\x1b[36m", "\x1b[34m"];
const RESET = "\x1b[0m";
const tag = (p) => `${COLOURS[p % COLOURS.length]}p${p}${RESET}`;

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers, logLevel: KafkaJS.logLevel.NOTHING },
});

const consumer = kafka.consumer({
  kafkaJS: {
    // 🔑 Same groupId in all three replicas = ONE group.
    //    Kafka hands each partition to exactly one member of a group.
    //    That's what makes them share the work instead of duplicating it.
    groupId: GROUP,
    fromBeginning: true,
  },

  // Fires whenever the group rebalances — i.e. whenever a consumer joins or
  // leaves. This is what you watch when you stop a container mid-demo.
  rebalance_cb: (err, assignment) => {
    const partitions = assignment.flatMap((a) =>
      typeof a.partition === "number" ? [a.partition] : (a.partitions ?? [])
    );
    const list = partitions.length ? partitions.sort().map(tag).join(" ") : "(none)";

    if (err.code === KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS) {
      console.log(`🎧 ${NAME} now owns partition(s): ${list}`);
    } else if (err.code === KafkaJS.ErrorCodes.ERR__REVOKE_PARTITIONS) {
      console.log(`↩️  ${NAME} gave up partition(s): ${list}`);
    }
  },
});

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

console.log(`🔵 ${NAME} joined group "${GROUP}" — waiting for an assignment…`);

// Leave the group cleanly on `docker compose stop`. Without this, Kafka only notices
// the consumer is gone after session.timeout.ms (45s) and its partition sits idle.
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    console.log(`👋 ${NAME} leaving the group…`);
    await consumer.disconnect();
    process.exit(0);
  });
}

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const order = JSON.parse(message.value);
    const key = message.key ? message.key.toString() : "<no key>";

    console.log(
      `${tag(partition)} offset ${String(message.offset).padEnd(3)} ` +
        `${NAME}  key=${key.padEnd(10)} → ${order.customerId} #${order.seq} ${order.item}`
    );
  },
});
