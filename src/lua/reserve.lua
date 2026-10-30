-- Atomic dedupe + quota check + charge for one metered request.
--
-- KEYS[1] limit_key        quota:{org}:feature:limit:period   (string, set by seed/admin)
-- KEYS[2] usage_key        quota:{org}:feature:usage:period   (string counter)
-- KEYS[3] reservation_key  res:{org}:request_id               (hash: state, fp, cost)
-- ARGV[1] cost             units this request charges
-- ARGV[2] fingerprint      hash of the request, to catch request_id reuse
-- ARGV[3] period_end       unix ts the usage counter expires at
-- ARGV[4] reservation_ttl  seconds; also the retry dedupe window
--
-- Returns {status, used, limit}. status is one of:
--   ok            charged; used/limit are post-charge values
--   reserved      retry of an in-flight request; nothing charged
--   committed     retry of a finished request; nothing charged
--   mismatch      request_id reused with a different fingerprint
--   unprovisioned no limit set for this org/feature/period
--   exceeded      cost would go over limit; used/limit are current values

local limit_key, usage_key, reservation_key = KEYS[1], KEYS[2], KEYS[3]
local cost = tonumber(ARGV[1])
local fingerprint = ARGV[2]
local period_end = ARGV[3]
local reservation_ttl = ARGV[4]

-- Dedupe: a reservation for this request_id already exists.
local state = redis.call('HGET', reservation_key, 'state')
if state then
  if redis.call('HGET', reservation_key, 'fp') ~= fingerprint then
    return {'mismatch', 0, 0}
  end
  return {state, 0, 0}
end

local limit = redis.call('GET', limit_key)
if not limit then
  return {'unprovisioned', 0, 0}
end
limit = tonumber(limit)

-- Check before charging so a rejected request writes nothing.
local used = tonumber(redis.call('GET', usage_key) or '0')
if used + cost > limit then
  return {'exceeded', used, limit}
end

used = redis.call('INCRBY', usage_key, cost)
-- TTL -1 means INCRBY just created the key: pin it to the period end
-- so the counter resets at the calendar boundary.
if redis.call('TTL', usage_key) == -1 then
  redis.call('EXPIREAT', usage_key, period_end)
end

-- Remember the charge so commit/refund know the cost and retries dedupe.
redis.call('HSET', reservation_key, 'state', 'reserved', 'fp', fingerprint, 'cost', cost)
redis.call('EXPIRE', reservation_key, reservation_ttl)
return {'ok', used, limit}
