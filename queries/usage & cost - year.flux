import "date"
import "timezone"
import "experimental"
import "array"
import "join"

option location = timezone.location(name: "$__timezone")

now_local = date.truncate(t: now(), unit: 1d)
this_month_start = date.truncate(t: now_local, unit: 1mo)
start = date.truncate(t: experimental.addDuration(d: -11mo, to: this_month_start), unit: 1mo)

// Generate the first day of each of the last 12 months
time_buckets = array.from(rows: [
  { _time: date.truncate(t: experimental.addDuration(d: -11mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -10mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -9mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -8mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -7mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -6mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -5mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -4mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -3mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -2mo, to: this_month_start), unit: 1mo) },
  { _time: date.truncate(t: experimental.addDuration(d: -1mo, to: this_month_start), unit: 1mo) },
  { _time: this_month_start }
])

// Compute kWh per month with correct output measure
energy_series = if "${override_output_measure}" == "ups.realpower" then (
  from(bucket: "${bucket}")
    |> range(start: start, stop: now_local)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.realpower")
    |> aggregateWindow(
          every: 1h,
          fn: (tables=<-, column) => tables |> integral(unit: 1h), 
          createEmpty: false
       )
    |> aggregateWindow(
          every: 1mo,
          fn: sum,
          createEmpty: false
       )
    |> map(fn: (r) => ({
        _time: date.truncate(t: r._time, unit: 1mo),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
) else (
  from(bucket: "${bucket}")
    |> range(start: start, stop: now_local)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.load")
    |> map(fn: (r) => ({
        _time: r._time,
        _value: (r._value / 100.0) * float(v: "${nominal_power_watts:0.0}")
    }))
    |> aggregateWindow(
          every: 1h,
          fn: (tables=<-, column) => tables |> integral(unit: 1h),
          createEmpty: false
       )
    |> aggregateWindow(
          every: 1mo,
          fn: sum,
          createEmpty: false
       )
    |> map(fn: (r) => ({
        _time: date.truncate(t: r._time, unit: 1mo),
        kWh: r._value / 1000.0
    }))
    |> keep(columns: ["_time", "kWh"])
)

// Left join energy data with all 12 buckets (to show months with no data as NaN)
kWh_series = join.left(
  left: time_buckets,
  right: energy_series,
  on: (l, r) => l._time == r._time,
  as: (l, r) => ({
    _time: l._time,
    kWh: if exists r.kWh then r.kWh else float(v: "NaN")
  })
)

// Calculate cost from kWh (also handles NaN)
cost_series = kWh_series
  |> map(fn: (r) => ({
    _time: r._time,
    cost: r.kWh * float(v: "${kWh_cost:0.0}")
  }))

// Output for Grafana
kWh_series  |> yield(name: "kWh")
cost_series |> yield(name: "cost")