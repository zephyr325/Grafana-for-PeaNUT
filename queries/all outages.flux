import "date"
import "timezone"
import "experimental"

option location = timezone.location(name: "${__timezone}")

polling_interval_s = int(v: "${polling_interval}")
buffer_s = 5
max_gap_s = polling_interval_s + buffer_s
polling_interval_ns = uint(v: polling_interval_s) * uint(v: 1000000000)

base = from(bucket: "${bucket}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) =>
    r._measurement == "${device}" and
    (r._field == "ups.status" or r._field == "input.transfer.reason")
  )
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r["ups.status"] == "OB DISCHRG")
  |> keep(columns: ["_time", "ups.status", "input.transfer.reason"])

with_elapsed = base
  |> elapsed(unit: 1s)
  |> map(fn: (r) => ({
    _time: r._time,
    ups_status: r["ups.status"],
    reason: r["input.transfer.reason"],
    gap_sec: int(v: r.elapsed),
    is_new_outage: if int(v: r.elapsed) > max_gap_s then 1.0 else 0.0
  }))

first = base
  |> limit(n: 1)
  |> map(fn: (r) => ({
    _time: r._time,
    ups_status: r["ups.status"],
    reason: r["input.transfer.reason"],
    gap_sec: -1,
    is_new_outage: 1.0
  }))

outages = union(tables: [first, with_elapsed])
  |> sort(columns: ["_time"])
  |> group(columns: [])

with_event_id = outages
  |> cumulativeSum(columns: ["is_new_outage"])
  |> rename(columns: {is_new_outage: "event_id"})

event_agg = with_event_id
  |> group(columns: ["event_id"])
|> reduce(
  identity: {
    event_start: time(v: uint(v: 9223372036854775807)),
    event_end: time(v: 0),
    transfer_reason: "",
  },
  fn: (r, accumulator) => ({
    event_start: if r._time < accumulator.event_start then r._time else accumulator.event_start,
    event_end: if r._time > accumulator.event_end then r._time else accumulator.event_end,
    transfer_reason: if r._time < accumulator.event_start then r.reason else accumulator.transfer_reason
  })
)
  |> map(fn: (r) => ({
    day: date.truncate(t: r.event_start, unit: 1d, location: location),
    event_start: r.event_start,
    event_end: experimental.addDuration(to: r.event_end, d: duration(v: polling_interval_ns)),
    duration: int(
      v: (uint(v: experimental.addDuration(to: r.event_end, d: duration(v: polling_interval_ns))) - uint(v: r.event_start))
        / uint(v: 1000000000)
    ),
    transfer_reason: r.transfer_reason
  }))

|> yield(name: "OB_Discharge_Outage_Events")