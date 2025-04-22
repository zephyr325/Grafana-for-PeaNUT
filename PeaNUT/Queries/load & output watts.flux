// 1. Power calculation logic (uses override_output_measure variable)
power_data = if "${override_output_measure}" == "ups.realpower" then (
    // Use measured real power
    from(bucket: "${bucket}")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) =>
        r._measurement == "${device}" and
        r._field == "ups.realpower"
      )
      |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
      |> map(fn: (r) => ({
        _time: r._time,
        _value: r._value,
        _field: "Measured Output",
        unit: "W"
      }))
) else (
    // Calculate real power from load percent
    from(bucket: "${bucket}")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) =>
        r._measurement == "${device}" and
        r._field == "ups.load"
      )
      |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
      |> map(fn: (r) => ({
        _time: r._time,
        _value: float(v: "${nominal_power_watts}") * (float(v: r._value) / 100.0),
        _field: "Calculated Output",
        unit: "W"
      }))
)

// 2. Load percentage series
load_data =
    from(bucket: "${bucket}")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) =>
        r._measurement == "${device}" and
        r._field == "ups.load"
    )
    |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
    |> map(fn: (r) => ({
        r with
        _field: "UPS Load",
        unit: "%"
    }))

// 3. Combined output
union(tables: [power_data, load_data])
  |> drop(columns: ["_start", "_stop", "_measurement"])
  |> yield()