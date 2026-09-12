/**
 * ───────────────────────────────────────────────────────────────────────
 *  Kafka's default partitioner — reimplemented so you can actually see it
 * ───────────────────────────────────────────────────────────────────────
 *
 *  ⚠️  Your app NEVER needs this file. Kafka already does this for you,
 *      inside the client library, on every send().
 *
 *      We reimplement it here for ONE reason: so the demo can PREDICT
 *      which partition a key will land on *before* sending it, and then
 *      prove the prediction was right. Teaching tool, not production code.
 *
 *  The entire rule, in one line:
 *
 *      partition = (murmur2(keyBytes) & 0x7fffffff) % partitionCount
 *
 *  Three things follow from that line, and they are the whole lesson:
 *
 *    1. It's a pure function of the key. Same key → same partition. Always.
 *       Not "usually". Not "unless a broker restarts". Always.
 *
 *    2. It knows nothing about your data. Two unrelated customers can
 *       absolutely share a partition. Partitions are buckets, not folders.
 *
 *    3. `% partitionCount` means the answer depends on the partition count.
 *       Change the count and every key remaps. (See README: "the one-way door".)
 *
 *  ── Is this the real algorithm? ──────────────────────────────────────
 *
 *  Yes. Verified two ways:
 *
 *    a) The 11 hash values in `REFERENCE_VECTORS` below are copied from
 *       librdkafka's own unit test (src/rdmurmur2.c), which in turn asserts
 *       compatibility with Apache Kafka's Java client `Utils.murmur2`.
 *       `npm run test:partitioner` checks this file against them.
 *
 *    b) The verifier service sends real messages to a real broker and
 *       asserts predicted partition === the partition the broker acked.
 *
 *  Both run in CI-style one-liners. See README → "Proving it's correct".
 */

/** Seed and constants are fixed by the Kafka protocol — do not change. */
const SEED = 0x9747b28c;
const M = 0x5bd1e995;
const R = 24;

/**
 * MurmurHash2, byte-for-byte identical to Apache Kafka's Java
 * `org.apache.kafka.common.utils.Utils.murmur2`.
 *
 * `Math.imul` is used for every multiply because JS numbers are doubles:
 * a plain `a * b` silently loses the low bits once the product passes 2^53.
 * `Math.imul` does a true 32-bit multiply, which is what the spec requires.
 * `>>> 0` after each step keeps the value an unsigned 32-bit int.
 *
 * @param {Buffer} buf raw key bytes
 * @returns {number} unsigned 32-bit hash
 */
export function murmur2(buf) {
  let len = buf.length;
  let h = (SEED ^ len) >>> 0;
  let i = 0;

  // Consume the key 4 bytes at a time, little-endian.
  while (len >= 4) {
    let k = (buf[i] | (buf[i + 1] << 8) | (buf[i + 2] << 16) | (buf[i + 3] << 24)) >>> 0;
    k = Math.imul(k, M) >>> 0;
    k = (k ^ (k >>> R)) >>> 0;
    k = Math.imul(k, M) >>> 0;
    h = Math.imul(h, M) >>> 0;
    h = (h ^ k) >>> 0;
    i += 4;
    len -= 4;
  }

  // Fold in the leftover 1-3 bytes. (Deliberate fallthrough in the original C.)
  if (len === 3) h = (h ^ (buf[i + 2] << 16)) >>> 0;
  if (len >= 2) h = (h ^ (buf[i + 1] << 8)) >>> 0;
  if (len >= 1) {
    h = (h ^ buf[i]) >>> 0;
    h = Math.imul(h, M) >>> 0;
  }

  // Final avalanche — scrambles the bits so similar keys land far apart.
  h = (h ^ (h >>> 13)) >>> 0;
  h = Math.imul(h, M) >>> 0;
  h = (h ^ (h >>> 15)) >>> 0;
  return h >>> 0;
}

/**
 * The default partitioner: which partition does this key go to?
 *
 * @param {string} key
 * @param {number} partitionCount
 * @returns {number} partition index, 0 .. partitionCount-1
 */
export function partitionFor(key, partitionCount) {
  const hash = murmur2(Buffer.from(key, "utf8"));
  // & 0x7fffffff clears the sign bit. Kafka's Java client stores the hash in a
  // signed int32, and a negative % would yield a negative partition index.
  return (hash & 0x7fffffff) % partitionCount;
}

/**
 * Same as partitionFor(), but shows its working — used by GET /api/where
 * so the arithmetic is visible on screen instead of being taken on faith.
 */
export function explain(key, partitionCount) {
  const bytes = Buffer.from(key, "utf8");
  const hash = murmur2(bytes);
  const positive = hash & 0x7fffffff;
  return {
    key,
    keyBytes: [...bytes].join(" "),
    murmur2: `0x${hash.toString(16).padStart(8, "0")}`,
    afterSignBitCleared: positive,
    partitionCount,
    partition: positive % partitionCount,
    formula: `(0x${hash.toString(16)} & 0x7fffffff) % ${partitionCount} = ${positive % partitionCount}`,
  };
}

/**
 * Hash values lifted verbatim from librdkafka's src/rdmurmur2.c unit test,
 * where they are labelled `java_murmur2_results`. If this file ever drifts
 * from the real algorithm, these stop matching.
 */
export const REFERENCE_VECTORS = [
  ["kafka", 0xd067cf64],
  ["giberish123456789", 0x8f552b0c],
  ["1234", 0x9fc97b14],
  ["234", 0xe7c009ca],
  ["34", 0x873930da],
  ["4", 0x5a4b5ca1],
  ["PreAmbleWillBeRemoved,ThePrePartThatIs", 0x78424f1c],
  ["reAmbleWillBeRemoved,ThePrePartThatIs", 0x4a62b377],
  ["eAmbleWillBeRemoved,ThePrePartThatIs", 0xe0e4e09e],
  ["AmbleWillBeRemoved,ThePrePartThatIs", 0x62b8b43f],
  ["", 0x106e08d9],
];
