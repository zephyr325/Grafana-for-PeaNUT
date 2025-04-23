import "timezone"
option location = timezone.location(name: "$__timezone")

energy_series = if "${override_output_measure}" == "ups.realpower" then (
  from(bucket: "${bucket}")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.realpower")
    |> aggregateWindow(
        every: 1m,
        fn: (tables=<-, column) => tables |> integral(unit: 1m),
        createEmpty: false,
        location: location
      )
    |> map(fn: (r) => ({
      _time: r._time,
      kWh: r._value / 1000.0
    }))
) else (
  from(bucket: "${bucket}")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "${device}" and r._field == "ups.load")
    |> map(fn: (r) => ({
      _time: r._time,
      _value: (r._value / 100.0) * float(v: "${nominal_power_watts:0.0}")
    }))
    |> aggregateWindow(
        every: 1m,
        fn: (tables=<-, column) => tables |> integral(unit: 1m),
        createEmpty: false,
        location: location
      )
    |> map(fn: (r) => ({
      _time: r._time,
      kWh: r._value / 1000.0
    }))
)

cost_series = energy_series
  |> map(fn: (r) => ({
    _time: r._time,
    cost: r.kWh * float(v: "${kWh_cost:0.0}")
  }))

energy_series |> yield(name: "kWh_hourly")
cost_series  |> yield(name: "cost_hourly")