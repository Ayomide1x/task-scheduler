-- token_bucket.lua
-- KEYS[1] = ratelimit:<client_id>
-- ARGV[1] = capacity (max tokens / burst size)
-- ARGV[2] = refill_rate (tokens per second)
-- ARGV[3] = now (unix timestamp, float)
-- ARGV[4] = ttl (seconds to keep an idle bucket around)
--
-- Must run as one atomic script: reading the current tokens, refilling,
-- checking >=1, and decrementing all have to happen as a single step. If
-- two requests from the same client arrive at the same instant and this
-- were split across separate round trips, both could read tokens=1, both
-- decide "allowed", and both decrement -- two requests admitted on a
-- bucket that only had room for one.

local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = ARGV[4]

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

tokens = math.min(capacity, tokens + math.max(0, now - last_refill) * refill_rate)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], ttl)

return {allowed, tostring(tokens)}
