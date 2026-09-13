-- promote_retries.lua
-- KEYS[1] = retry:scheduled  (sorted set: member = job id, score = eligible unix ts)
-- KEYS[2] = queue:pending
-- ARGV[1] = current unix timestamp
-- ARGV[2] = max number of due jobs to promote in this call
--
-- Must run as one atomic script: finding which ids are due, removing them
-- from retry:scheduled, and pushing them onto queue:pending all have to
-- happen as a single indivisible step. If two workers each read the same
-- due id and both remove+push it independently, the id ends up in
-- queue:pending twice -- two separate list entries with the same job id,
-- which two separate BLMOVE calls can each claim, running the same job
-- concurrently. That's exactly what stage 2's claiming exists to prevent,
-- so this can't be split across round trips.
--
-- ARGV[2] bounds how many ids one call will process. Redis executes scripts
-- single-threaded and blocks everything else while one runs, so an
-- unbounded ZRANGEBYSCORE here (many thousands of due jobs at once) would
-- stall the whole server for the duration of the loop below.

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job_id in ipairs(due) do
  redis.call('ZREM', KEYS[1], job_id)
  redis.call('LPUSH', KEYS[2], job_id)
end
return due
