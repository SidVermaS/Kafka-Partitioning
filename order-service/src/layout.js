/**
 * Renders the proof table shown by GET /api/layout.
 *
 * Every partition/offset printed here came back from the BROKER in the
 * send() acknowledgement. Nothing on this screen is a guess.
 */

const WIDTH = 72;

const line = (l, r) => l + "─".repeat(WIDTH - 2) + r;
const row = (text = "") => {
  // Emoji render two columns wide in a terminal; the variation selector renders zero.
  const visual = [...text].reduce(
    (n, ch) => n + (ch === "️" ? 0 : /\p{Extended_Pictographic}/u.test(ch) ? 2 : 1),
    0
  );
  return "│ " + text + " ".repeat(Math.max(0, WIDTH - 3 - visual)) + "│";
};

export function renderLayout(ledger, partitionCount) {
  const out = [];
  out.push(line("┌", "┐"));
  out.push(row(`topic "orders"  ·  ${partitionCount} partitions  ·  ${ledger.length} messages`));
  out.push(row());

  if (ledger.length === 0) {
    out.push(row("(nothing sent yet — try: curl -XPOST localhost:3000/api/demo)"));
    out.push(line("└", "┘"));
    return out.join("\n") + "\n";
  }

  // ── What landed in each partition ──────────────────────────────────
  for (let p = 0; p < partitionCount; p++) {
    const msgs = ledger.filter((m) => m.partition === p).sort((a, b) => a.offset - b.offset);
    out.push(row(`partition ${p}  ${"▓".repeat(msgs.length).padEnd(10, "░")}  ${msgs.length} msg`));
    for (const m of msgs) {
      const who = m.keyed ? `${m.customerId} #${m.seq}` : `<no key>`;
      out.push(row(`   offset ${String(m.offset).padEnd(3)} ${who.padEnd(14)} ${m.item ?? ""}`));
    }
    out.push(row());
  }

  // ── Per-customer verdict: was ordering preserved? ──────────────────
  out.push(line("├", "┤"));
  out.push(row("Per-customer ordering"));
  out.push(row());

  const customers = [...new Set(ledger.map((m) => m.customerId))];
  for (const c of customers) {
    const mine = ledger.filter((m) => m.customerId === c);
    const parts = [...new Set(mine.map((m) => m.partition))].sort();
    const keyed = mine.every((m) => m.keyed);
    // Within one partition, offsets are assigned in append order — ascending
    // offsets mean the broker stored them in the order we sent them.
    const ascending = mine.every((m, i) => i === 0 || Number(m.offset) > Number(mine[i - 1].offset));

    let icon, verdict;
    if (parts.length > 1) {
      icon = "❌";
      verdict = `split across p${parts.join(", p")} — ORDER LOST`;
    } else if (!keyed) {
      // One message, or a lucky streak — either way nothing guaranteed it.
      icon = "⚠️";
      verdict = `on p${parts[0]} by luck — no key, no guarantee`;
    } else {
      icon = ascending ? "✅" : "❌";
      verdict = `all on partition ${parts[0]} — order kept`;
    }

    const count = `${String(mine.length).padStart(2)} ${mine.length === 1 ? "order " : "orders"}`;
    out.push(row(`  ${icon} ${c.padEnd(8)} ${count}  ${verdict}`));
  }

  // ── Did our local prediction match the broker? ─────────────────────
  const keyedMsgs = ledger.filter((m) => m.keyed);
  if (keyedMsgs.length) {
    const matched = keyedMsgs.filter((m) => m.predicted === m.partition).length;
    out.push(row());
    out.push(
      row(
        `  ${matched === keyedMsgs.length ? "✅" : "❌"} partitioner.js predicted ` +
          `${matched}/${keyedMsgs.length} partitions correctly`
      )
    );
  }

  out.push(line("└", "┘"));
  return out.join("\n") + "\n";
}
