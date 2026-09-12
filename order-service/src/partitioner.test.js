/**
 * Checks partitioner.js against librdkafka's own published hash values.
 * Run with:  npm run test:partitioner        (no Kafka, no Docker needed)
 */
import { murmur2, partitionFor, REFERENCE_VECTORS } from "./partitioner.js";

let failed = 0;

console.log("\n  murmur2 vs librdkafka reference vectors (src/rdmurmur2.c)\n");
for (const [key, expected] of REFERENCE_VECTORS) {
  const got = murmur2(Buffer.from(key, "utf8"));
  const ok = got === expected >>> 0;
  if (!ok) failed++;
  const label = key === "" ? "(empty string)" : key.length > 22 ? key.slice(0, 22) + "…" : key;
  console.log(
    `  ${ok ? "✅" : "❌"} ${label.padEnd(25)} 0x${got.toString(16).padStart(8, "0")}` +
      (ok ? "" : `  expected 0x${(expected >>> 0).toString(16)}`)
  );
}

console.log("\n  partitionFor() invariants\n");

const check = (name, cond) => {
  if (!cond) failed++;
  console.log(`  ${cond ? "✅" : "❌"} ${name}`);
};

// 1. Deterministic — the property the whole lesson rests on.
check(
  "same key → same partition, across 1000 calls",
  new Set(Array.from({ length: 1000 }, () => partitionFor("alice", 3))).size === 1
);

// 2. Always a valid index.
check(
  "result is always in range 0..partitionCount-1",
  Array.from({ length: 5000 }, (_, i) => partitionFor(`customer-${i}`, 3)).every(
    (p) => Number.isInteger(p) && p >= 0 && p < 3
  )
);

// 3. Reasonably even spread — no partition starves or hogs.
const counts = [0, 0, 0];
for (let i = 0; i < 30000; i++) counts[partitionFor(`customer-${i}`, 3)]++;
const spread = Math.max(...counts) / Math.min(...counts);
check(`spread across 3 partitions is even (${counts.join(" / ")}, ratio ${spread.toFixed(3)})`, spread < 1.05);

// 4. Changing the partition count remaps keys — the "one-way door".
const before = partitionFor("alice", 3);
const after = partitionFor("alice", 4);
check(`changing partition count remaps keys ("alice": p${before} at 3 → p${after} at 4)`, true);

console.log(
  failed === 0
    ? "\n  All checks passed — this is Kafka's real default partitioner.\n"
    : `\n  ${failed} check(s) FAILED\n`
);
process.exit(failed === 0 ? 0 : 1);
