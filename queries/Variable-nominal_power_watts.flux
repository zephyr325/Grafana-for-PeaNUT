fallback_val = from(bucket: "${bucket}")
  |> range(start: -1h)
  |> filter(fn: (r) =>
    r._measurement == "${device}" and
    r._field == "ups.realpower.nominal"
  )
  |> last()
  |> findRecord(fn: (key) => true, idx: 0)

override_val = "${override_ups_maxwatts}"

nominal = if override_val == "" then float(v: fallback_val._value) else float(v: override_val)

// Wrap in dummy table for Grafana variable support
from(bucket: "${bucket}")
  |> range(start: -1m)
  |> limit(n:1)
  |> map(fn: (r) => ({ _value: nominal }))