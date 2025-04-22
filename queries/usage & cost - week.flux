import "date"
import "experimental"
import "timezone"
import "array"
import "join"

option location = timezone.location(name: "$__timezone")
now_local = date.truncate(t: now(), unit: 1d)
start_7d = date.add(d: -7d, to: now_local)
start_8d = date.add(d: -8d, to: now_local)

// Generate 7 day buckets, starting from 7 days ago up to yesterday
day_buckets = array.from(rows: [
  { _time: start_7d },
  { _time: date.add(d: 1d, to: start_7d) },
  { _time: date.add(d: 2d, to: start_7d) },
  { _time: date.add(d: 3d, to: start_7d) },
  { _time: date.add(d: 4d, to: start_7d) },
  { _time: date.add(d: 5d, to: start_7d) },
  { _time: date.add(d: 6d, to: start_7d) },
])

// Get the right data source for output power
energy_series = if "${override_output_measure}" == "ups.realpower" then (
  from(bucket: "${bucket}")
    |> range(start: start_8d, stop: now_local)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.realpower")
    |> aggregateWindow(
          every: 1h,
          fn: (tables=<-, column) => tables |> integral(unit: 1h),
          createEmpty: false
       )
    |> aggregateWindow(
          every: 1d,
          fn: sum,
          createEmpty: false,
          location: location
       )
    |> map(fn: (r) => ({
        // _time is end of window: shift -1d to represent start of day
        _time: date.add(d: -1d, to: date.truncate(t: r._time, unit: 1d)),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
) else (
  from(bucket: "${bucket}")
    |> range(start: start_8d, stop: now_local)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.load")
    |> map(fn: (r) => ({
        _time: r._time,
        _value: (r._value / 100.0) * float(v: "${nominal_power_watts:0.0}")
    }))
    |> aggregateWindow(
          every: 1h,
          fn: (tables=<-, column) => tables |> integral(unit: 1h),
          createEmpty: false,
          location: location
       )
    |> aggregateWindow(
          every: 1d,
          fn: sum,
          createEmpty: false,
          location: location
       )
    |> map(fn: (r) => ({
        // _time is end of window: shift -1d to represent start of day
        _time: date.add(d: -1d, to: date.truncate(t: r._time, unit: 1d)),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
)

// Left join onto days: Set NaN for missing days
kWh_series = join.left(
  left: day_buckets,
  right: energy_series,
  on: (l, r) => l._time == r._time,
  as: (l, r) => ({
    _time: l._time,
    kWh: if exists r.kWh then r.kWh else float(v: "NaN")
  })
)

// Compute cost
cost_series = kWh_series
  |> map(fn: (r) => ({
    _time: r._time,
    cost: r.kWh * float(v: "${kWh_cost:0.0}")
  }))

// Output
kWh_series  |> yield(name: "kWh")
cost_series |> yield(name: "cost")