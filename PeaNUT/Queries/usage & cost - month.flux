import "date"
import "timezone"
import "experimental"
import "array"
import "join"
option location = timezone.location(name: "$__timezone")

now_local = date.truncate(t: now(), unit: 1d)
first_day_this_month = date.truncate(t: now_local, unit: 1mo)

// Generate up to 31 possible days for this month
day_buckets = array.from(rows: [
  { _time: first_day_this_month },
  { _time: date.add(d: 1d, to: first_day_this_month) },
  { _time: date.add(d: 2d, to: first_day_this_month) },
  { _time: date.add(d: 3d, to: first_day_this_month) },
  { _time: date.add(d: 4d, to: first_day_this_month) },
  { _time: date.add(d: 5d, to: first_day_this_month) },
  { _time: date.add(d: 6d, to: first_day_this_month) },
  { _time: date.add(d: 7d, to: first_day_this_month) },
  { _time: date.add(d: 8d, to: first_day_this_month) },
  { _time: date.add(d: 9d, to: first_day_this_month) },
  { _time: date.add(d: 10d, to: first_day_this_month) },
  { _time: date.add(d: 11d, to: first_day_this_month) },
  { _time: date.add(d: 12d, to: first_day_this_month) },
  { _time: date.add(d: 13d, to: first_day_this_month) },
  { _time: date.add(d: 14d, to: first_day_this_month) },
  { _time: date.add(d: 15d, to: first_day_this_month) },
  { _time: date.add(d: 16d, to: first_day_this_month) },
  { _time: date.add(d: 17d, to: first_day_this_month) },
  { _time: date.add(d: 18d, to: first_day_this_month) },
  { _time: date.add(d: 19d, to: first_day_this_month) },
  { _time: date.add(d: 20d, to: first_day_this_month) },
  { _time: date.add(d: 21d, to: first_day_this_month) },
  { _time: date.add(d: 22d, to: first_day_this_month) },
  { _time: date.add(d: 23d, to: first_day_this_month) },
  { _time: date.add(d: 24d, to: first_day_this_month) },
  { _time: date.add(d: 25d, to: first_day_this_month) },
  { _time: date.add(d: 26d, to: first_day_this_month) },
  { _time: date.add(d: 27d, to: first_day_this_month) },
  { _time: date.add(d: 28d, to: first_day_this_month) },
  { _time: date.add(d: 29d, to: first_day_this_month) },
  { _time: date.add(d: 30d, to: first_day_this_month) }
])

// Calculate daily kWh (for this month, excluding today)
energy_series = if "${override_output_measure}" == "ups.realpower" then (
  from(bucket: "${bucket}")
    |> range(start: first_day_this_month, stop: now_local)
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
    // Set _time to the **start** of the window
    |> map(fn: (r) => ({
        _time: date.add(d: -1d, to: date.truncate(t: r._time, unit: 1d)),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
) else (
  from(bucket: "${bucket}")
    |> range(start: first_day_this_month, stop: now_local)
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
    // Set _time to the **start** of the window
    |> map(fn: (r) => ({
        _time: date.add(d: -1d, to: date.truncate(t: r._time, unit: 1d)),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
)

// Left join energy to day_buckets, set NaN for days with no data
kWh_series = join.left(
  left: day_buckets,
  right: energy_series,
  on: (l, r) => l._time == r._time,
  as: (l, r) => ({
    _time: l._time,
    kWh: if exists r.kWh then r.kWh else float(v: "NaN")
  })
)
|> filter(fn: (r) => r._time < now_local)  // only include days before today
|> filter(fn: (r) => r._time >= first_day_this_month and r._time < now_local) // in current month
// Step 4: Calculate daily cost
cost_series = kWh_series
  |> map(fn: (r) => ({
    _time: r._time,
    cost: r.kWh * float(v: "${kWh_cost:0.0}")
  }))

// Output for Grafana
kWh_series  |> yield(name: "kWh")
cost_series |> yield(name: "cost")