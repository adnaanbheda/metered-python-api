-- Give back a reservation's cost and drop it, so a retry is a fresh request.
--
-- KEYS[1] reservation_key  res:{org}:request_id (hash: state, fp, cost)
-- KEYS[2] usage_key        quota:{org}:feature:usage:period
--
-- Returns 1 if refunded, 0 if the reservation wasn't in 'reserved' state
-- (already committed, refunded, or expired).

local reservation_key, usage_key = KEYS[1], KEYS[2]

if redis.call('HGET', reservation_key, 'state') ~= 'reserved' then
  return 0
end

-- Only decrement a live counter: after period rollover the key is gone, and
-- DECRBY would recreate it as a negative value with no TTL.
if redis.call('EXISTS', usage_key) == 1 then
  redis.call('DECRBY', usage_key, redis.call('HGET', reservation_key, 'cost'))
end
redis.call('DEL', reservation_key)
return 1
