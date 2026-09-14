#!/usr/bin/env node
// REST gateway: the only process besides workers that talks to Redis
// directly. The CLI (stage 6) talks to this over HTTP and never touches
// Redis itself -- see CLAUDE.md ("two paths into job state will drift").
//
// This gateway is itself a second writer of the job schema, alongside
// worker.py, and duplicates one of worker.py's constants
// (HEARTBEAT_TIMEOUT_SECONDS) in a second language. See DECISIONS.md
// ("Cross-language duplication: job schema and HEARTBEAT_TIMEOUT_SECONDS")
// for what that risks and what the real fix would be.
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const { createClient } = require("redis");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379/0";
const PORT = process.env.PORT || 3000;

// Duplicated from worker.py -- see DECISIONS.md. Keep in sync by hand.
const HEARTBEAT_TIMEOUT_SECONDS = 15;

const RATE_LIMIT_CAPACITY = 10;
const RATE_LIMIT_REFILL_PER_SECOND = 1;
const RATE_LIMIT_TTL_SECONDS = Math.ceil(RATE_LIMIT_CAPACITY / RATE_LIMIT_REFILL_PER_SECOND);

const DEFAULT_DLQ_LIMIT = 20;

const TOKEN_BUCKET_SCRIPT = fs.readFileSync(path.join(__dirname, "token_bucket.lua"), "utf8");

const app = express();
app.use(express.json());

// Explicitly NOT trusting X-Forwarded-For: this stack has no reverse proxy
// in front of the gateway (see DECISIONS.md, stage 5 design). With
// trust proxy off, req.socket.remoteAddress is the actual TCP peer address,
// which a client cannot simply set to a different value on each request --
// unlike a header, which it could. If a real proxy is ever added in front
// of this gateway, that's the point to revisit this, not before.
app.set("trust proxy", false);

const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("[gateway] redis error:", err));

function jobResponse(jobId, hash) {
  const job = { job_id: jobId, ...hash };
  if (job.args !== undefined) job.args = JSON.parse(job.args);
  if (job.result !== undefined) job.result = JSON.parse(job.result);
  for (const field of ["attempts", "reclaims", "claimed_at", "next_attempt_at"]) {
    if (job[field] !== undefined) job[field] = Number(job[field]);
  }
  return job;
}

async function rateLimit(req, res, next) {
  const clientId = req.socket.remoteAddress;
  const now = Date.now() / 1000;

  const [allowed, tokensRemaining] = await redis.eval(TOKEN_BUCKET_SCRIPT, {
    keys: [`ratelimit:${clientId}`],
    arguments: [
      String(RATE_LIMIT_CAPACITY),
      String(RATE_LIMIT_REFILL_PER_SECOND),
      String(now),
      String(RATE_LIMIT_TTL_SECONDS),
    ],
  });

  if (Number(allowed) === 1) {
    next();
    return;
  }

  // tokensRemaining is negative-of-deficit here (< 1, e.g. 0.3): the time
  // until the bucket reaches 1 token is the deficit divided by the refill
  // rate. Rounded up and floored at 1s so a client never gets Retry-After: 0.
  const deficit = 1 - Number(tokensRemaining);
  const retryAfterSeconds = Math.max(1, Math.ceil(deficit / RATE_LIMIT_REFILL_PER_SECOND));
  res.set("Retry-After", String(retryAfterSeconds));
  res.status(429).json({ error: "rate limit exceeded", retry_after_seconds: retryAfterSeconds });
}

app.post("/jobs", rateLimit, async (req, res) => {
  const { task, args } = req.body || {};

  if (typeof task !== "string" || task.length === 0) {
    res.status(400).json({ error: "task must be a non-empty string" });
    return;
  }
  const jobArgs = args === undefined ? [] : args;
  if (!Array.isArray(jobArgs)) {
    res.status(400).json({ error: "args must be an array" });
    return;
  }

  // Deliberately not validating that `task` is a registered task name --
  // the gateway has no visibility into tasks.py's REGISTRY (different
  // language, different process), and the worker already rejects an
  // unknown task correctly (straight to DLQ, no wasted retries -- stage 3).
  // Duplicating that check here would just be a second list to keep in
  // sync for no functional benefit.
  const jobId = crypto.randomUUID();

  // Same two writes submit_job.py used to make, now the gateway's job --
  // see DECISIONS.md on why submit_job.py is retired now that this exists.
  await redis.hSet(`job:${jobId}`, {
    task,
    args: JSON.stringify(jobArgs),
    status: "pending",
    created_at: new Date().toISOString(),
  });
  await redis.lPush("queue:pending", jobId);

  res.status(201).json({ job_id: jobId });
});

app.get("/jobs/:id", async (req, res) => {
  const hash = await redis.hGetAll(`job:${req.params.id}`);
  if (Object.keys(hash).length === 0) {
    res.status(404).json({ error: "job not found" });
    return;
  }
  res.json(jobResponse(req.params.id, hash));
});

app.get("/stats", async (req, res) => {
  const now = Date.now() / 1000;
  const [pending, dlq, workersAlive] = await Promise.all([
    redis.lLen("queue:pending"),
    redis.lLen("queue:dead"),
    redis.zCount("workers:heartbeats", now - HEARTBEAT_TIMEOUT_SECONDS, "+inf"),
  ]);
  res.json({ pending, dlq, workers_alive: workersAlive });
});

app.get("/dlq", async (req, res) => {
  const limit = Math.max(1, parseInt(req.query.limit, 10) || DEFAULT_DLQ_LIMIT);

  const [ids, total] = await Promise.all([
    redis.lRange("queue:dead", 0, limit - 1),
    redis.lLen("queue:dead"),
  ]);

  const jobs = await Promise.all(
    ids.map(async (jobId) => {
      const fields = await redis.hmGet(`job:${jobId}`, ["task", "attempts", "reclaims", "last_error"]);
      return {
        job_id: jobId,
        task: fields[0],
        attempts: fields[1] === null ? null : Number(fields[1]),
        reclaims: fields[2] === null ? null : Number(fields[2]),
        last_error: fields[3],
      };
    })
  );

  res.json({ jobs, count: jobs.length, total });
});

async function main() {
  await redis.connect();
  app.listen(PORT, () => {
    console.log(`[gateway] listening on port ${PORT}`);
  });
}

main();
