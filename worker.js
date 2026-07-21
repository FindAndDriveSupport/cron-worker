/**
 * cron-worker — Cloudflare Worker
 *
 * "Finds new leads." Two internal stages, each its own invocation type, so
 * per-branch subrequest budget is never shared (the exact problem that
 * caused the original single-invocation design to fail as dealer count grew):
 *
 * 1. DISPATCH (scheduled, every 30 min): reads dealer configs from
 *    LEADS_SYNC_CONFIG, expands each into its branches, and enqueues ONE
 *    lightweight message per branch onto branch-fetch-queue. Zero external
 *    API calls — just KV reads — so this step can never run out of budget
 *    regardless of dealer count. Runs every 30 min (not 5) — see "QUEUES
 *    OPERATIONS BUDGET" note below for why.
 * 2. BRANCH-FETCH (queue consumer, one branch per invocation via
 *    max_batch_size: 1): authenticates with Seriti (SERITI_TOKEN_CACHE),
 *    fetches highIntent/lowIntent leads over a small rolling date window,
 *    runs Kredo enrichment once per high-intent lead if enabled, and calls
 *    queue-worker DIRECTLY via Service Binding (not a queue — see below)
 *    for each newly-discovered lead.
 *
 * This Worker does destination-agnostic dedup at the LEAD level (see
 * "DEDUP" note below) and NO destination delivery — that's queue-worker
 * and the delivery workers' job, reached via Service Bindings.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * QUEUES OPERATIONS BUDGET — why branch-fetch-queue is the ONLY real queue left
 * ─────────────────────────────────────────────────────────────────────────
 * Cloudflare Queues costs ~3 operations per message (write+read+delete),
 * with a 10k/day budget on the free plan. The original 4-queue design
 * (dispatch → branch-fetch-queue → discovered-leads-queue → queue-worker →
 * integration-queue/digest-accumulate-queue) blew through that budget from
 * dispatch fan-out ALONE: 30 branches × 288 ticks/day (5-min cron) × 3 ops
 * = ~25,920 ops/day, 2.6x over budget, before counting a single lead.
 *
 * Two fixes, both applied:
 *   1. This Worker now calls queue-worker via Service Binding (a direct
 *      Worker-to-Worker fetch() call) instead of sending a queue message.
 *      Service Binding calls are NOT Queues operations — they're billed as
 *      ordinary Workers requests instead, so all per-LEAD traffic (the
 *      expensive, volume-scaling part) is now free against the 10k/day cap.
 *      integration-worker and digest-worker were converted the same way —
 *      see their own file headers.
 *   2. branch-fetch-queue is DELIBERATELY KEPT as a real queue — it's the
 *      one hand-off that genuinely needs queue semantics (fan-out to many
 *      isolated invocations, each with its own subrequest budget). Its
 *      cost at 30-min ticks: 30 branches × 48 ticks/day × 3 ops = 4,320
 *      ops/day — comfortably under budget with headroom to grow.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * DEDUP — cron-worker now has its own lead-level marker (CRON_FORWARD_MARKER_TTL)
 * ─────────────────────────────────────────────────────────────────────────
 * Without this, forwardLeads() would call queue-worker for the SAME lead on
 * every single tick for its entire 2-day stay in the rolling fetch window —
 * wasted Kredo submissions, wasted downstream calls, even though
 * queue-worker's own per-destination dedup would eventually stop it from
 * reaching HubSpot/CMS/email a second time. This marker is separate from
 * queue-worker's per-destination LEADS_SYNC_CACHE keys — it answers "have I
 * already forwarded this raw lead to queue-worker", not "has this specific
 * destination received it". Written ONLY on a successful forward — a
 * failed Service Binding call leaves it unmarked, so the next tick retries
 * automatically (the replacement for the queue's built-in per-message retry).
 *
 * ─────────────────────────────────────────────────────────────────────────
 * QUEUE SEND RATE LIMITING (dispatch → BRANCH_FETCH_QUEUE.sendBatch)
 * ─────────────────────────────────────────────────────────────────────────
 * This exact failure took leads-api down for most of a day: sendBatch()
 * throwing "Too Many Requests" on every single 5-minute tick, uncaught,
 * killing the entire dispatch run before later chunks ever sent. Fixed two
 * ways, both preserved here:
 *   1. Each chunk's send is independently fault-tolerant — dispatch()'s
 *      loop catches per-chunk failures and moves on to the next chunk
 *      rather than throwing and aborting everything after it.
 *   2. sendBatchWithRetry() backs off up to ~25s+ across 5 retries with
 *      jitter, not the original ~3.5s across 3 — that was too short to let
 *      whatever throttle window this is actually clear, given it was
 *      failing on every single tick rather than occasionally.
 * If retries are STILL exhausting on every tick even with this longer
 * backoff, that's a sign of a genuine sustained rate ceiling for this
 * account/plan — check the actual current limit in the Cloudflare
 * dashboard (Queues → branch-fetch-queue → Settings) rather than assuming
 * any specific number.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * ROLLING DATE WINDOW (fetchSeritiLeads)
 * ─────────────────────────────────────────────────────────────────────────
 * Queries today minus LOOKBACK_DAYS through today — NOT a dealer's original
 * onboarding startDate through today. The old behaviour meant every sync,
 * forever, re-fetched the entire history since onboarding, growing larger
 * every day even though downstream dedup discards almost everything in it.
 * 2 days (not 1) deliberately overlaps the previous day so a lead dated
 * right at a midnight boundary (or skewed by the UTC-vs-SAST 2hr offset)
 * never falls through a gap between cron ticks. A dealer's stored
 * startDate is no longer used for regular syncs — kept in config only for
 * reference / manual backfill purposes (no backfill mechanism currently
 * exists — flagged as an open gap, not solved here).
 *
 * ─────────────────────────────────────────────────────────────────────────
 * MESSAGE / CALL CONTRACTS
 * ─────────────────────────────────────────────────────────────────────────
 * Produced onto branch-fetch-queue (internal, consumed by this same Worker):
 *   { dealerKey, branchCode, seritiApiKey, seritiApiSecret,
 *     seritiDealershipId, kredoEnabled, kredoUsername, kredoPassword,
 *     kredoXApiKey, destinations }
 *   — destinations already has shared CMS/VMG credentials merged in at
 *     dispatch time (one KV read per dealer covers every branch).
 *
 * POST to queue-worker via Service Binding (env.QUEUE_WORKER.fetch(...)),
 * NOT a queue message — see "QUEUES OPERATIONS BUDGET" above:
 *   { dealerKey, branchCode, intent, lead, approvalChance, destinations }
 *   — ONE call per lead, not per lead-destination pair. Synchronous —
 *     this Worker awaits the response before marking the lead forwarded.
 *
 * REQUIRED wrangler.toml:
 *   [triggers] crons = ["every 30 minutes" cron expression, e.g. star-slash-30 star star star star]
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CONFIG"
 *   [[kv_namespaces]] binding = "SERITI_TOKEN_CACHE"
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"   ← NEW, for the lead-level dedup marker
 *   [[queues.producers]] binding = "BRANCH_FETCH_QUEUE" queue = "branch-fetch-queue"
 *   [[queues.consumers]] queue = "branch-fetch-queue"
 *     max_batch_size = 1   ← CRITICAL. >1 reintroduces shared-budget sharing.
 *     max_retries = 3, dead_letter_queue = "branch-fetch-dlq"
 *   [[queues.consumers]] queue = "branch-fetch-dlq"
 *     max_batch_size = 10, max_retries = 3
 *   [[services]] binding = "QUEUE_WORKER" service = "queue-worker"   ← NEW
 *
 * CONFIRMED: Seriti auth tokens last 1 hour. SERITI_TOKEN_CACHE_TTL below
 * caches for 55 minutes, same margin used for VMG's own token cache
 * elsewhere in this pipeline.
 */

import crypto from "node:crypto";

const SHARED_CREDENTIALS_KEY = "__shared_credentials__";
const SERITI_TOKEN_CACHE_TTL = 3300; // 55 min — Seriti tokens confirmed to last 1 hour.
const LOOKBACK_DAYS = 2;             // see file header "ROLLING DATE WINDOW".
const MAX_BRANCH_SYNC_RETRIES = 3;   // keep in sync with branch-fetch-queue's max_retries in wrangler.toml.

function getRollingDateRange() {
  const endDate = new Date().toISOString().slice(0, 10);
  const start = new Date();
  start.setUTCDate(start.getUTCDate() - LOOKBACK_DAYS);
  const startDate = start.toISOString().slice(0, 10);
  return { startDate, endDate };
}

export default {
  async scheduled(event, env, ctx) {
    await dispatch(env);
  },

  async fetch(request, env, ctx) {
    if (new URL(request.url).pathname === "/run") {
      await dispatch(env);
      return new Response("Dispatch complete", { status: 200 });
    }
    return new Response("cron-worker", { status: 200 });
  },

  async queue(batch, env, ctx) {
    if (batch.queue === "branch-fetch-dlq") {
      return handleDeadLetterBatch(batch);
    }
    return handleBranchFetchBatch(batch, env);
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// STAGE 1 — DISPATCH: enqueue one message per branch. No API calls.
// ═══════════════════════════════════════════════════════════════════════════
async function dispatch(env) {
  console.log("🚀 Dispatch starting — enqueueing per-branch fetch jobs...");

  let sharedCreds = {};
  const sharedRaw = await env.LEADS_SYNC_CONFIG.get(SHARED_CREDENTIALS_KEY);
  if (sharedRaw) {
    try {
      sharedCreds = JSON.parse(sharedRaw);
    } catch {
      console.error(`❌ Invalid JSON in shared credentials entry (${SHARED_CREDENTIALS_KEY}) — falling back to per-dealer credentials only.`);
    }
  }

  const { keys } = await env.LEADS_SYNC_CONFIG.list();
  const dealerKeys = keys.filter(({ name }) => name !== SHARED_CREDENTIALS_KEY);
  if (!dealerKeys.length) {
    console.log("ℹ️  No dealer configs found in LEADS_SYNC_CONFIG KV.");
    return;
  }

  let totalBranches = 0;
  const messages = [];

  for (const { name } of dealerKeys) {
    const raw = await env.LEADS_SYNC_CONFIG.get(name);
    if (!raw) continue;

    let dealer;
    try {
      dealer = JSON.parse(raw);
    } catch {
      console.error(`❌ Invalid JSON for dealer config: ${name}`);
      continue;
    }

    const branches = getEffectiveBranches(dealer);
    if (branches.length === 0) {
      console.error(`⚠️  Dealer ${dealer.key} has no destinations/branches configured — skipping.`);
      continue;
    }

    const {
      key,
      seritiApiKey,
      seritiApiSecret,
      kredoEnabled = false,
      kredoUsername,
      kredoPassword,
      kredoXApiKey,
    } = dealer;

    for (const branch of branches) {
      const { branchCode, seritiDealershipId, destinations } = branch;
      const resolvedDestinations = destinations.map((d) => resolveDestinationCredentials(d, sharedCreds));

      messages.push({
        body: {
          dealerKey: key,
          branchCode,
          seritiApiKey,
          seritiApiSecret,
          seritiDealershipId,
          kredoEnabled,
          kredoUsername,
          kredoPassword,
          kredoXApiKey,
          destinations: resolvedDestinations,
        },
      });
      totalBranches++;
    }
  }

  // Chunk into smaller batches than Queues' own 100-message max — sending
  // everything in one large sendBatch() call risks hitting a per-request
  // burst/throughput limit even when the DAILY operations budget is fine
  // (confirmed in production: a single 30-message sendBatch() call got
  // "Too Many Requests" even after moving dispatch to a 30-min interval,
  // which had already fixed the separate daily-budget problem). A smaller
  // chunk size, with a brief pause between chunks, spreads the same total
  // message count across multiple smaller bursts instead of one large one.
  // Each chunk is still independently fault-tolerant — see file header
  // "QUEUE SEND RATE LIMITING" note.
  const CHUNK_SIZE = 10;
  const CHUNK_DELAY_MS = 300;

  let successfullyEnqueued = 0;
  let chunksFailed = 0;
  const totalChunks = Math.ceil(messages.length / CHUNK_SIZE) || 0;

  for (let i = 0; i < messages.length; i += CHUNK_SIZE) {
    const chunk = messages.slice(i, i + CHUNK_SIZE);
    const chunkNum = Math.floor(i / CHUNK_SIZE) + 1;
    try {
      await sendBatchWithRetry(env, chunk);
      successfullyEnqueued += chunk.length;
    } catch (err) {
      chunksFailed++;
      console.error(
        `❌ Chunk ${chunkNum}/${totalChunks} (${chunk.length} branch job(s)) failed to enqueue after all retries: ${err.message}. ` +
        `These branches were NOT dispatched this cycle — they'll be attempted again automatically on the next dispatch (~30 min).`
      );
    }

    // Brief pause between chunks — gives any per-second/per-request burst
    // ceiling room to reset before the next sendBatch() call, rather than
    // firing every chunk back-to-back as fast as the loop can go.
    if (i + CHUNK_SIZE < messages.length) {
      await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY_MS));
    }
  }

  if (chunksFailed > 0) {
    console.error(`⚠️  Dispatch finished with ${chunksFailed}/${totalChunks} chunk(s) failed — ${totalBranches - successfullyEnqueued} branch job(s) not enqueued this cycle.`);
  }

  console.log(`✅ Dispatch complete. Enqueued ${successfullyEnqueued}/${totalBranches} branch fetch job(s).`);
}

async function sendBatchWithRetry(env, batch, retries = 5) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      await env.BRANCH_FETCH_QUEUE.sendBatch(batch);
      return;
    } catch (err) {
      const isRateLimit = /too many requests|429/i.test(err.message || '');
      if (isRateLimit && attempt < retries) {
        const baseDelay = Math.min(1000 * Math.pow(2, attempt), 10000);
        const jitter = Math.random() * 500;
        const delayMs = Math.round(baseDelay + jitter);
        console.log(`⚠️  BRANCH_FETCH_QUEUE.sendBatch got rate-limited (attempt ${attempt + 1}/${retries + 1}), retrying in ${delayMs}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        continue;
      }
      throw err;
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// STAGE 2 — BRANCH-FETCH: one branch per invocation.
// ═══════════════════════════════════════════════════════════════════════════

async function handleBranchFetchBatch(batch, env) {
  for (const message of batch.messages) {
    const { dealerKey, branchCode } = message.body;
    const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
    try {
      const discoveredCount = await syncBranch(message.body, env);
      console.log(`✅ [branch-fetch] ${label} — sent ${discoveredCount} lead(s) to queue-worker.`);
      message.ack();
    } catch (err) {
      const isFinalAttempt = message.attempts > MAX_BRANCH_SYNC_RETRIES;
      if (isFinalAttempt) {
        console.error(`❌ [DEAD LETTER] Branch fetch permanently failed for ${label} after ${message.attempts} attempts: ${err.message}`);
      } else {
        console.log(`⚠️  [branch-fetch] Attempt ${message.attempts} failed for ${label}, will retry: ${err.message}`);
      }
      message.retry();
    }
  }
}

async function handleDeadLetterBatch(batch) {
  for (const message of batch.messages) {
    const { dealerKey, branchCode } = message.body;
    const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
    console.error(`❌ [DEAD LETTER QUEUE] Branch fetch job for ${label} landed in DLQ. Needs manual review — check Seriti credentials/dealer config for this branch.`);
    message.ack();
  }
}

// Fetches leads for ONE branch, enriches with Kredo if applicable, and
// forwards each newly-discovered lead to queue-worker via Service Binding.
// This invocation has its own full subrequest
// budget — no sharing with any other branch or dealer.
async function syncBranch(job, env) {
  const {
    dealerKey, branchCode, seritiApiKey, seritiApiSecret, seritiDealershipId,
    kredoEnabled, kredoUsername, kredoPassword, kredoXApiKey, destinations,
  } = job;
  const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;

  const seritiToken = await getSeritiToken(dealerKey, seritiApiKey, seritiApiSecret, env);
  const { startDate, endDate } = getRollingDateRange();

  const [highResult, lowResult] = await Promise.allSettled([
    fetchSeritiLeads("highIntent", seritiDealershipId, startDate, endDate, seritiToken),
    fetchSeritiLeads("lowIntent", seritiDealershipId, startDate, endDate, seritiToken),
  ]);

  let discoveredCount = 0;

  if (highResult.status === "fulfilled") {
    console.log(`── High Intent: ${label} (${highResult.value.length} leads) ──`);
    highResult.value.forEach(l => console.log(`  🔍 lead dealerCode="${l.dealerCode}" dealerName="${l.dealerName}" (requested seritiDealershipId=${seritiDealershipId})`));
    discoveredCount += await forwardLeads(highResult.value, "highIntent", dealerKey, branchCode, destinations, env, {
      runKredo: kredoEnabled, kredoUsername, kredoPassword, kredoXApiKey,
    });
  } else {
    console.error(`❌ highIntent fetch failed for ${label}:`, highResult.reason.message);
  }

  if (lowResult.status === "fulfilled") {
    console.log(`── Low Intent: ${label} (${lowResult.value.length} leads) ──`);
    discoveredCount += await forwardLeads(lowResult.value, "lowIntent", dealerKey, branchCode, destinations, env, {
      runKredo: false, // low-intent never runs Kredo, unchanged from every previous version of this pipeline.
    });
  } else {
    console.error(`❌ lowIntent fetch failed for ${label}:`, lowResult.reason.message);
  }

  // If BOTH intent fetches failed, treat the whole branch job as failed so
  // the queue retries it — a single transient double-failure shouldn't
  // silently produce "0 leads forwarded" and get ack'd as success.
  if (highResult.status === "rejected" && lowResult.status === "rejected") {
    throw new Error(`Both highIntent and lowIntent fetches failed for ${label}`);
  }

  return discoveredCount;
}

// Runs Kredo once per lead (not per destination) if enabled, then calls
// queue-worker directly via Service Binding — carrying the full
// destinations array. Deduplicated at the LEAD level (not per-destination
// — that's still queue-worker's job) via a dedicated cron-worker marker,
// so the same lead isn't re-forwarded on every tick for its whole 2-day
// stay in the rolling window. Marker is only written on a SUCCESSFUL
// forward — a failed call leaves it unmarked, so the next tick (up to 30
// min later) naturally retries it. This is the retry mechanism now that
// there's no queue providing one automatically.
const CRON_FORWARD_MARKER_TTL = 259200; // 3 days — safely beyond the 2-day rolling window, so a forwarded lead is never reconsidered while it's still in-window.

async function forwardLeads(leads, intent, dealerKey, branchCode, destinations, env, kredoOpts) {
  let sentCount = 0;

  for (const lead of leads) {
    const uniqueId = lead.idNumber || lead.mobileNumber || "unknown";
    const forwardKey = `fwd-${dealerKey}-${branchCode || "default"}-${intent}-${uniqueId}-${lead.date}`;

    const alreadyForwarded = await env.LEADS_SYNC_CACHE.get(forwardKey);
    if (alreadyForwarded) continue;

    let approvalChance = lead.approvalChance ?? null;

    if (kredoOpts.runKredo) {
      try {
        const kredoResult = await submitToKredo(lead, kredoOpts);
        approvalChance = String(
          kredoResult?.data?.report?.predictor?.vehicle_asset_finance?.PredictedApproval
            ?? approvalChance
        );
      } catch (err) {
        console.error(`  ❌ Kredo submission failed for ${lead.firstName} ${lead.lastName}, forwarding without enrichment: ${err.message}`);
      }
    }

    try {
      const res = await env.QUEUE_WORKER.fetch("https://internal/process-lead", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dealerKey, branchCode, intent, lead, approvalChance, destinations }),
      });

      if (!res.ok) {
        throw new Error(`queue-worker responded ${res.status}`);
      }

      await env.LEADS_SYNC_CACHE.put(forwardKey, "1", { expirationTtl: CRON_FORWARD_MARKER_TTL });
      sentCount++;
    } catch (err) {
      console.error(`  ❌ Failed to forward lead ${lead.firstName} ${lead.lastName} to queue-worker: ${err.message}`);
      // Don't mark the forward key — next dispatch cycle (up to 30 min
      // later) will retry this same lead, since it's still unmarked and
      // still within the rolling fetch window.
    }
  }

  return sentCount;
}

// ─── Seriti auth (KV-cached, per-dealer) ────────────────────────────────────
async function getSeritiToken(dealerKey, apiKey, apiSecret, env, retries = 2) {
  const cacheKey = `seriti-token-${dealerKey}`;
  const cached = await env.SERITI_TOKEN_CACHE.get(cacheKey);
  if (cached) return cached;

  for (let attempt = 0; attempt <= retries; attempt++) {
    console.log(`🔑 Authenticating with Seriti for ${dealerKey}... [attempt ${attempt + 1}]`);

    const res = await fetch("https://seritiapi.findndrive.co.za/api/Authentication/token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ApiKeyId: apiKey, ApiSecret: apiSecret }),
    });

    if (res.ok) {
      const data = await res.json();
      const token = data.token || data.access_token || data.accessToken;
      if (!token) throw new Error(`Seriti auth — no token in response: ${JSON.stringify(data)}`);
      console.log(`✅ Seriti token acquired for ${dealerKey}.`);
      await env.SERITI_TOKEN_CACHE.put(cacheKey, token, { expirationTtl: SERITI_TOKEN_CACHE_TTL });
      return token;
    }

    if (res.status >= 500 && attempt < retries) {
      const delayMs = 500 * (attempt + 1);
      console.log(`⚠️  Seriti auth got ${res.status}, retrying in ${delayMs}ms...`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      continue;
    }

    throw new Error(`Seriti auth failed: ${res.status}`);
  }
}

// ─── Seriti fetch leads ───────────────────────────────────────────────────────
async function fetchSeritiLeads(intent, dealershipId, startDate, endDate, token, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    console.log(`📡 Fetching ${intent} leads (${startDate} → ${endDate})... [attempt ${attempt + 1}]`);

    const res = await fetch(
      `https://seritiapi.findndrive.co.za/api/Leads/${intent}/${dealershipId}?startDate=${startDate}&endDate=${endDate}`,
      { headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } }
    );

    if (res.ok) {
      const leads = await res.json();
      console.log(`✅ ${leads.length} ${intent} lead(s) returned.`);
      return leads;
    }

    if (res.status >= 500 && attempt < retries) {
      const delayMs = 500 * (attempt + 1);
      console.log(`⚠️  ${intent} fetch got ${res.status}, retrying in ${delayMs}ms...`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      continue;
    }

    throw new Error(`Seriti leads fetch failed: ${res.status}`);
  }
}

// ─── Kredo credit check ───────────────────────────────────────────────────────
async function submitToKredo(lead, opts) {
  console.log(`  🔍 Submitting ${lead.firstName} ${lead.lastName} to Kredo...`);

  const authRes = await fetch("https://api.kredo.co.za/private/client/user/auth", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": opts.kredoXApiKey },
    body: JSON.stringify({ username: opts.kredoUsername, password: opts.kredoPassword }),
  });
  if (!authRes.ok) throw new Error(`Kredo auth failed: ${authRes.status}`);
  const authData = await authRes.json();
  const kredoToken = authData.authorizationToken || authData.token || authData.access_token;
  if (!kredoToken) throw new Error(`Kredo auth — no token: ${JSON.stringify(authData)}`);

  const creditRes = await fetch("https://api.kredo.co.za/credit-report-json", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": opts.kredoXApiKey,
      "authorizationToken": kredoToken,
    },
    body: JSON.stringify({
      client_guid: crypto.randomUUID(),
      consumer: {
        id_number: lead.idNumber,
        first_name: lead.firstName,
        last_name: lead.lastName,
        cell_number: lead.mobileNumber,
        work_number: "",
        home_number: "",
        email_address: "",
        gross_income: Number(lead.netIncome) || 0,
        household_expenses: 0,
        reason: "Affordability Assessment",
        consent: true,
      },
    }),
  });
  if (!creditRes.ok) throw new Error(`Kredo credit report failed: ${creditRes.status}`);
  const result = await creditRes.json();
  console.log(`  ✅ Kredo report received.`);
  return result;
}

// ─── Shared credentials + branch normalization ─────────────────────────────
function resolveDestinationCredentials(dest, sharedCreds) {
  if (dest.type === "cms") {
    return { ...dest, cmsToken: dest.cmsToken || sharedCreds?.cms?.cmsToken };
  }
  if (dest.type === "vmg") {
    return {
      ...dest,
      vmgUsername: dest.vmgUsername || sharedCreds?.vmg?.vmgUsername,
      vmgPassword: dest.vmgPassword || sharedCreds?.vmg?.vmgPassword,
    };
  }
  return dest; // hubspot, email stay per-dealer only.
}

function getEffectiveBranches(dealer) {
  if (Array.isArray(dealer.branches) && dealer.branches.length > 0) {
    return dealer.branches;
  }
  if (Array.isArray(dealer.destinations) && dealer.destinations.length > 0) {
    return [{
      branchCode: null,
      seritiDealershipId: dealer.seritiDealershipId,
      destinations: dealer.destinations,
    }];
  }
  return [];
}
