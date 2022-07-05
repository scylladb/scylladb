*******************************
Adding and Modifying Dashboards
*******************************

The following document explains how to update or create a Grafana dashboard for the Scylla Monitoring stack.

It will explain about Scylla monitoring templated dashboards and how to modify them.


General Limitations
###################
Scylla Monitoring Stack uses Grafana for its dashboards. As of Grafana version 5.0, there are limitations
to the changes, you can make to dashboards.

You can create your own dashboard and modify it with the GUI, but you will face a consistency issue (keeping the dashboard between Grafana container restart).

At large, we suggest maintaining your dashboards as files, as Scylla Monitoring Stack does.

.. note::  You cannot save a dashboard that was created as a file when you modify it with the GUI.


Using templated Dashboards
##########################
Why Scylla Monitoring uses templated dashboards?
 
We found the Grafana dashboards Json format too verbose to be maintainable.

Each element in the dashboard file (Each Json object) contains all of its attributes and values.

For example, a typical graph panel would look like:

.. code-block:: json

        {
            "aliasColors": {},
            "bars": false,
            "datasource": "prometheus",
            "editable": true,
            "error": false,
            "fill": 0,
            "grid": {
                "threshold1": null,
                "threshold1Color": "rgba(216, 200, 27, 0.27)",
                "threshold2": null,
                "threshold2Color": "rgba(234, 112, 112, 0.22)"
            },
            "gridPos": {
                "h": 6,
                "w": 10,
                "x": 0,
                "y": 4
            },
            "id": 2,
            "isNew": true,
            "legend": {
                "avg": false,
                "current": false,
                "max": false,
                "min": false,
                "show": false,
                "total": false,
                "values": false
            },
            "lines": true,
            "linewidth": 2,
            "links": [],
            "nullPointMode": "connected",
            "percentage": false,
            "pointradius": 5,
            "points": false,
            "renderer": "flot",
            "seriesOverrides": [
                {}
            ],
            "span": 5,
            "stack": false,
            "steppedLine": false,
            "targets": [
                {
                    "expr": "sum(node_filesystem_avail) by (instance)",
                    "intervalFactor": 1,
                    "legendFormat": "",
                    "refId": "A",
                    "step": 1
                }
            ],
            "timeFrom": null,
            "timeShift": null,
            "title": "Available Disk Size",
            "tooltip": {
                "msResolution": false,
                "shared": true,
                "sort": 0,
                "value_type": "cumulative"
            },
            "transparent": false,
            "type": "graph",
            "xaxis": {
                "show": true
            },
            "yaxes": [
                {
                    "format": "percent",
                    "logBase": 1,
                    "max": 101,
                    "min": 0,
                    "show": true
                },
                {
                    "format": "short",
                    "logBase": 1,
                    "max": null,
                    "min": null,
                    "show": true
                }
            ]
        }

As you can imagine, most panels would have similar values. 

To reduce the redundancy of Grafana JSON format, we added templated dashboards. 

The template classes system
***************************
Scylla Monitoring templates use ``class`` attribute that can be added to any JSON object in a template file.
The different classes are defined in a file.

The ``class`` system resembles CSS classes. It is hierarchical, so a ``class`` type definition can have a ``class`` attribute and
it would inherit that class attributes. The inherited class can add or override inherited attributes.

In the template file, you can also add or override attributes.

Scylla Monitoring Stack generation script uses the `types.json` file and a template file and creates a dashboard.

When generating dashboards, each class will be replaced by its definition.

For example row in the `type.json` defined

.. code-block:: json

   {
    "base_row": {
        "collapse": false,
        "editable": true
    },
    "row": {
        "class": "base_row",
        "height": "250px"
    }
    }

In a Template it will be used like

.. code-block:: json

   {
        "class": "row",
        "height": "150px",
        "panels": [
        ]
   }

The output will be

.. code-block:: json

   {
        "class": "row",
        "collapse": false,
        "editable": true,
        "height": "150px",
        "panels": [
    
        ]
   }


We can see that the template added the ``panels`` attribute and override the ``height`` attribute.


Panel example
*************

Look at the following example that defines a row inside a dashboard with a graph
panel for the available disk size.

.. code-block:: json

   {
        "class": "row",
        "panels": [
            {
                "class": "bytes_panel",
                "span": 3,
                "targets": [
                    {
                        "expr": "sum(node_filesystem_avail) by (instance)",
                        "intervalFactor": 1,
                        "legendFormat": "",
                        "metric": "",
                        "refId": "A",
                        "step": 1
                    }
                ],
                "title": "Available Disk Size"
            }
        ]
   }

In the example, we used the `bytes_panel` class that generates a graph with bytes as units (that would mean that your
`Y` axis unit would adjust themselves to make the graph readable (i.e. GB, MB, bytes, etc')  

You can also see that we override the `span` attribute to set the panel size.

To get a grasp of the difference, take a look at the Grafana panel example, and see how it looks originally.

Grafana Formats and Layouts
***************************
Grafana layout used to be based on rows. Each contains multiple panels.
Each row would have a total span of 12, if the total span of the panels be larger than 12, it would
break the lines into multiple lines.

Starting from  Grafana version 5.0 and later, rows are no longer supported, it was replaced with a layout that uses
absolute positions (ie. X,Y, height, width).

The server should be backward compatible, but we've found it had issues with parsing it correctly.
More so, absolute positions are impossible to get right when done by hand.

To overcome these issues, the dashboard generation script will generate the dashboards in the Grafana version 5.0 format.
In the transition, rows will be replaced with a calculated absolute position.

Panel's height will be taken from their row. The `span` attribute is still supported like you expect it to work, so does row height.

You can use the `gridPos` attribute, which is a Grafana 5.0 format, but not like Grafana, you can use partial attributes.

`gridPos` has the following attributes:

.. code-block:: json

   {
      "x": 0,
      "y": 0,
      "w": 24,
      "h": 4
    } 

When using Scylla's template, you don't need to supply them all, so for example, to specify that a row is 2 units height, you can use:

.. code-block:: json

    {
       "gridPos": {
          "h": 2
        } 
    }

Generating the dashboards from templates (generate-dashboards.sh)
*****************************************************************

prerequisite
============
Python 2.7


`make_dashboards.py` is a utility that generates dashboards from templates or helps you update the template when working in reverse mode (the `-r` flag).

Use the -h flag to get help information.

You can use the `make_dashboards.py` to generate a single dashboard, but it's usually easier to use the
`generate-dashboards.sh` wrapper.

When you're done changing an existing dashboard template, run the `generate-dashboards.sh` with the current version,
this will replace your existing dashboards.

For example, if you are changing a dashboard in Scylla Enterprise version 2018.1 run:

``.\generate-dashboards.sh -v 2018.1``

.. note::  generate-dashboards.sh will update the dashboards in place, no need for a restart for the changes to take effect, just refresh the dashboard.


Validation
**********
After making changes to a template, run the ``generate_generate-dashboards.sh`` you should see that it run without any errors.

Refresh your browser for changes to take effect.
Make sure that your panels contain data. If not, maybe there is something wrong with your ``expr`` attribute.
