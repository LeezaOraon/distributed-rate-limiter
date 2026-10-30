-- KEYS[1] = base key for this client, e.g. "rl:user:123"
-- ARGV[1] = now_ms        (current timestamp in milliseconds)
-- ARGV[2] = window_ms     (window size in milliseconds, e.g. 60000)
-- ARGV[3] = limit         (max requests allowed per window)
--
-- Returns: { allowed (1 or 0), estimated_count (after this request if allowed),
--            retry_after_ms (0 if allowed) }

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
  -- Work out when this key would next be admitted, assuming no further
  -- requests arrive. Admitting needs: previous * (window_ms - e) / window_ms
  -- <= limit - 1 - current, where e is the elapsed time in a window.
  local room = limit - 1 - current_count
  local wait_ms
  local need_e = window_ms
  if room >= 0 and previous_count > 0 then
    need_e = window_ms - (room * window_ms / previous_count)
  end
  if room >= 0 and need_e < window_ms then
    -- the previous window's weight decays enough inside this window
    wait_ms = need_e - elapsed_ms
  else
    -- wait for the next window, where this window's count is the "previous"
    local e_next = 0
    if current_count > 0 then
      e_next = math.max(0, window_ms - ((limit - 1) * window_ms / current_count))
    end
    wait_ms = (window_ms - elapsed_ms) + e_next
  end
  return { 0, math.floor(estimated_count), math.max(1, math.ceil(wait_ms)) }
end

local new_count = redis.call("INCR", current_key)
if new_count == 1 then
  -- first request in this window bucket: set it to expire after
  -- 2 windows, so it stays around long enough to serve as the
  -- "previous window" for the next bucket, then cleans itself up
  redis.call("PEXPIRE", current_key, window_ms * 2)
end

return { 1, math.floor(estimated_count + 1), 0 }
