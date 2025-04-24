# Grafana-for-PeaNUT
This is a Grafana dashboard extending [PeaNUT's](https://github.com/Brandawg93/PeaNUT) native capability to push UPS statistics to an InfluxDB v 2.x database while providing functioning panels to create your own personal dashboard.  

![](/images/peanut_grafana_main.png)

## Features

* Multiple types of pre-built Grafana panels representing a variety of metrics including base UPS operations, output power, power costs, and other miscellanous information
* Week, month, and annual aggregations using hourly integral metrics to increase precision
* Timezone-aware queries (automatically set to the browser's timezone) ensuring that the default Influx UTC-based time data reflects the local time when aggregating data
* Multiple methods to show recent outage information
* Automatic implementation of power output measurement through the ups load percent (more commonly available, but less precise) while also providing a single-click change to actual power output (less commonly available, but more precise)
* Other inputs and overrides include cost/kWh and actual maximum load wattage
* Queries have been optimized to reduce load/refresh times, and uses template variables for static or rarely-changing values
* PeaNUT is based on the industry-standard [Network UPS Tools (NUT)](https://networkupstools.org/) - all database fields reflect that standard 

### Pre-Built Panels
![](/images/power_costs_only.png)
![](/images/power_consumption_only.png)
![](/images/power_consumption_and_cost_style_1.png)
![](/images/power_consumption_and_cost_style_2.png)
![](/images/ups_operations.png)
![](/images/miscellaneous.png)
![](/images/all_fields_last_value.png)

## Usage


### Importing the Dashboard
1. Ensure that your PeaNUT instance is correctly communicating with your InfluxDB database.  For more information on that, see [here](https://github.com/Brandawg93/PeaNUT/wiki/YAML-Configuration).
2. Download the [dashboard JSON file](https://github.com/zephyr325/Grafana-for-PeaNUT/archive/refs/tags/v1.0.0.zip).
3. In your Grafana instance, go to Dashboards and click on the "^" character in the "New" button.  This will give you the ability to import a JSON file.
4. Use the "Upload dashboard JSON file" box to select the dashboard file, and click "Load"
5. Select your InfluxDB database, and click "Import"

### Customizing Your Dashboard
Dashboards tend to be driven by your personal preferences of both functionality and style; this dashboard has pre-built panels for a range of information and styles to choose from.  Implementing this is fairly straightfoward:

* The top area already includes a few dashboard panels.
* All the template panels are in the rows below that area.  After clicking the "Edit" button (top right of the page), either just drag the panels you want to the top, or you can duplicate a panel (top right 3 dots --> More --> Duplicate) and move that to the top.
* Arrange and resize the panels to your preference.
* Minimize or delete the rows once you're done; this will prevent unecessary server-side load and your dashboard page's load/refresh time.
* Note that several of the panels and variables have information in the information section; hover over the circled "i" to see that.

## Acknowledgements & Other Info
This dashboard was conceptualized and based on the hours of work by [gilbn](https://grafana.com/grafana/dashboards/10914-unraid-nut-ups-dashboard-tr/) and [artstar](https://grafana.com/grafana/dashboards/15010-apc-ups-detailed-summary/).  Many thanks for that time and effort!