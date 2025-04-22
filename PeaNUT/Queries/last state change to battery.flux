import "date"
import "timezone"
import "experimental"

option location = timezone.location(name: "${__timezone}")

polling_interval_s = int(v: "${polling_interval}")
buffer_s = 5
max_gap_s = polling_interval_s + buffer_s
polling_interval_ns = uint(v: polling_interval_s) * uint(v: 1000000000)
one_day_ns = uint(v: 86400000000000)
now_local = date.truncate(t: now(), unit: 1d, location: location)
start_7d = time(v: uint(v: now_local) - (one_day_ns * uint(v: 6))) 
stop_time = date.add(d: duration(v: one_day_ns), to: now_local)

// Base data: get both ups.status and input.transfer.reason together
base = from(bucket: "${bucket}")
  |> range(start: start_7d, stop: stop_time)
  |> filter(fn: (r) =>
    r._measurement == "${device}" and
    (r._field == "ups.status" or r._field == "input.transfer.reason")
  )
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r["ups.status"] == "OB DISCHRG")
  |> sort(columns: ["_time"])
  |> keep(columns: ["_time", "ups.status", "input.transfer.reason"])

// with_elapsed: calculate elapsed time and mark new outages, keep transfer_reason
with_elapsed = base
  |> elapsed(unit: 1s)
  |> map(fn: (r) => ({
    _time: r._time,
    ups_status: r["ups.status"],
    transfer_reason: r["input.transfer.reason"],
    gap_sec: int(v: r.elapsed),
    is_new_outage: if int(v: r.elapsed) > max_gap_s then 1.0 else 0.0
  }))

// first outage record, initialize fields with transfer_reason
first = base
  |> limit(n: 1)
  |> map(fn: (r) => ({
    _time: r._time,
    ups_status: r["ups.status"],
    transfer_reason: r["input.transfer.reason"],
    gap_sec: -1,
    is_new_outage: 1.0
  }))

outages = union(tables: [first, with_elapsed])
  |> sort(columns: ["_time"])
  |> group(columns: [])

// cumulative sum to create event ids
with_event_id = outages
  |> cumulativeSum(columns: ["is_new_outage"])
  |> rename(columns: {is_new_outage: "event_id"})

// Aggregate start/end times and pick transfer_reason from first event record
event_agg = with_event_id
  |> group(columns: ["event_id"])
  |> reduce(
    identity: {
      event_start: time(v: uint(v: 9223372036854775807)),
      event_end: time(v: 0),
      transfer_reason: ""
    },
    fn: (r, accumulator) => ({
      event_start: if r._time < accumulator.event_start then r._time else accumulator.event_start,
      event_end: if r._time > accumulator.event_end then r._time else accumulator.event_end,
      // update transfer_reason only if this record is earlier than stored event_start and transfer_reason exists
      transfer_reason: if r._time < accumulator.event_start and exists r.transfer_reason and r.transfer_reason != "" then r.transfer_reason else accumulator.transfer_reason
    })
  )
  |> map(fn: (r) => ({
    string1: "Last Outage: ",
    string2: ", Duration (",
    string3: " seconds), Reason: ",
    event_start: r.event_start,
    duration: float(
      v: uint(v: experimental.addDuration(to: r.event_end, d: duration(v: polling_interval_ns))) - uint(v: r.event_start)
    ) / 1000000000.0,
    transfer_reason: r.transfer_reason
  }))
  |> sort(columns: ["event_start"], desc: true)
  |> limit(n: 1)

// Yield with reason included
event_agg
  |> yield(name: "OB_Discharge_Outage_Events_With_Reason")