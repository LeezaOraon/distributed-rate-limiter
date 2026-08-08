-- KEYS[1] = base key for this client, e.g. "rl:user:123"
-- ARGV[1] = now_ms        (current timestamp in milliseconds)
-- ARGV[2] = window_ms     (window size in milliseconds, e.g. 60000)
-- ARGV[3] = limit         (max requests allowed per window)
--
-- Returns: { allowed (1 or 0), estimated_count_after_this_request }

local base_key   = KEYS[1]
local now_ms     = tonumber(ARGV[1])
local window_ms  = tonumber(ARGV[2])
local limit      = tonumber(ARGV[3])

local current_window  = math.floor(now_ms / window_ms)
local previous_window = current_window - 1
local elapsed_ms       = now_ms - (current_window * window_ms)

local current_key  = base_key .. ":" .. current_window
local previous_key = base_key .. ":" .. previous_window

local current_count  = tonumber(redis.call("GET", current_key)) or 0
local previous_count = tonumber(redis.call("GET", previous_key)) or 0

local weight = (window_ms - elapsed_ms) / window_ms
local estimated_count = (previous_count * weight) + current_count

if estimated_count + 1 > limit then
  return { 0, estimated_count }
end

local new_count = redis.call("INCR", current_key)
if new_count == 1 then
  -- first request in this window bucket: set it to expire after
  -- 2 windows, so it stays around long enough to serve as the
  -- "previous window" for the next bucket, then cleans itself up
  redis.call("PEXPIRE", current_key, window_ms * 2)
end

return { 1, estimated_count + 1 }
