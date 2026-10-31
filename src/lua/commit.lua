-- Finalize a reservation and log the usage event in one atomic step,
-- so there's never a committed reservation without an event (or vice versa).
--
-- KEYS[1] reservation_key  res:{org}:request_id (hash: state, fp, cost)
-- KEYS[2] stream_key       usage event stream
-- ARGV[1] org_id
-- ARGV[2] feature
-- ARGV[3] request_id
-- ARGV[4] period           e.g. 2026-10
-- ARGV[5] stream_maxlen    approximate cap on stream length
-- ARGV[6] instance         which service instance committed (its hostname)
--
-- Returns 1 if committed, 0 if the reservation wasn't in 'reserved' state
-- (already committed, refunded, or expired).

local reservation_key, stream_key = KEYS[1], KEYS[2]
local org_id, feature, request_id, period, stream_maxlen, instance =
  ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]

if redis.call('HGET', reservation_key, 'state') ~= 'reserved' then
  return 0
end

-- HSET keeps the key's TTL, so retries keep deduping until it expires.
redis.call('HSET', reservation_key, 'state', 'committed')
redis.call('XADD', stream_key, 'MAXLEN', '~', stream_maxlen, '*',
  'org_id', org_id, 'feature', feature, 'request_id', request_id,
  'period', period, 'cost', redis.call('HGET', reservation_key, 'cost'),
  'instance', instance)
return 1
