import "date"
import "timezone"
import "experimental"
import "array"
option location = timezone.location(name: "${__timezone}")
polling_interval_s = int(v: "${polling_interval}")
buffer_s = 5
max_gap_s = polling_interval_s + buffer_s
polling_interval_ns = uint(v: polling_interval_s) * uint(v: 1000000000)
one_day_ns = uint(v: 86400000000000)
now_local = date.truncate(t: now(), unit: 1d, location: location)
start_7d = time(v: uint(v: now_local) - (one_day_ns * uint(v: 6))) 
stop_time = date.add(d: duration(v: one_day_ns), to: now_local)
base = from(bucket: "${bucket}")
  |> range(start: start_7d, stop: stop_time)
  |> filter(fn: (r) =>
    r._measurement == "${device}" and
    r._field == "ups.status" and
    r._value == "OB DISCHRG"
  )
  |> sort(columns: ["_time"])
  |> keep(columns: ["_time", "_value"])
with_elapsed = base
  |> elapsed(unit: 1s)
  |> map(fn: (r) => ({
    _time: r._time,
    _value: r._value,
    gap_sec: int(v: r.elapsed),
    is_new_outage: if int(v: r.elapsed) > max_gap_s then 1.0 else 0.0
  }))
first = base
  |> first()
  |> map(fn: (r) => ({
    _time: r._time,
    _value: r._value,
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
    },
    fn: (r, accumulator) => ({
      event_start: if r._time < accumulator.event_start then r._time else accumulator.event_start,
      event_end: if r._time > accumulator.event_end then r._time else accumulator.event_end
    })
  )
  |> map(fn: (r) => ({
    day: date.truncate(t: r.event_start, unit: 1d, location: location),
    event_start: r.event_start,
    event_end: experimental.addDuration(to: r.event_end, d: duration(v: polling_interval_ns)),
    duration: uint(
      v: uint(v: experimental.addDuration(to: r.event_end, d: duration(v: polling_interval_ns))) - uint(v: r.event_start)
    )
  }))
  |> group()

days = union(tables: [
  event_agg,
  array.from(rows: [
    {day: date.truncate(t: now_local, unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - one_day_ns), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - (one_day_ns * uint(v: 2))), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - (one_day_ns * uint(v: 3))), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - (one_day_ns * uint(v: 4))), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - (one_day_ns * uint(v: 5))), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)},
    {day: date.truncate(t: time(v: uint(v: now_local) - (one_day_ns * uint(v: 6))), unit: 1d, location: location), event_start: time(v: 0), event_end: time(v: 0), duration: uint(v: 0)}
  ])
])

clean = days
  |> drop(columns: ["event_start", "event_end"])

with_counts = clean
  |> group(columns: ["day"])
  |> map(fn: (r) => ({
      day: r.day,
      duration: r.duration,
      is_real: if r.duration > 0 then 1 else 0
  }))
  |> group(columns: ["day"])
  |> reduce(
      identity: { day: time(v: 0), count: 0, has_real: false },
      fn: (r, accumulator) => ({
          day: r.day,
          count: if r.duration > 0 then accumulator.count + 1 else accumulator.count,
          has_real: accumulator.has_real or (r.duration > 0)
      })
  )
  |> map(fn: (r) => ({
      day: r.day,
      count: if r.has_real then r.count else 0 // If no real outages, 0
  }))
  |> sort(columns:["day"])
  |> group()  // <--- Group ALL into one table
with_counts
  |> yield(name: "OB_Discharge_Daily_Counts")