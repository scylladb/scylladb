# Logging in Scylla
Scylla implements logging using Seastar's logging facilities. Each log message comes with a severity level increasing from "trace", to "debug", "info", "warn" and "error", and with a subsystem name. The developer can filter messages based on these levels and system name - for example ask to show a large number of "trace" messages for a subsystem he or she is currently debugging, while other subsystems will only display serious warnings or error messages.

Log messages are sent to Scylla's standard output and/or to the system log, as specified by the `--log-to-stdout` and `--log-to-syslog` options. The default is to send log messages to stdout only, but Scylla is installed we override this default so that log messages are sent to the system log. When the output is to stdout, the `--logger-stdout-timestamps <type>` option can be used to control the addition of time stamps to log messages: The default type of "real" prints the current time in human-readable date form. The type "boot" prints the time as the (floating-point) number of seconds since the epoch of std::chrono::steady_clock. The type "none" does not add a timestamp to log messages.

As in any Seastar application, one can control Scylla's log filtering on startup with various command-line options. By default, log messages with severity **info** or above (info, warn or error) are displayed and the rest are filtered out. This default filtering level can be adjusted with the "`--default-log-level <level>`" option. But more commonly, a developer wants to enable more detailed logging just for selected subsystems. This can be done with a "`--logger-log-level <subsystem>=<level>' option. For example, `--logger-log-level repair=debug` to enable log messages from the "repair" subsystem if their severity level is `debug` or higher.

The command line option "--help-loggers" can be used to get a list of all known subsystems.

While for short-running tests it is easy enough to set the log filtering at the beginning of the run, in longer-running scenarios it is often useful to change the log filtering during the run. In Scylla, we can do this through the REST API:

To obtain the status of a particular logger:

```
$ curl -X GET http://127.0.0.1:10000/system/logger/<subsystem_name>
```

For example: 

```
$ curl -X GET http://127.0.0.1:10000/system/logger/sstable
```

To change the status of a particular logger:

```
$ curl -X POST http://127.0.0.1:10000/system/logger/sstable?level=trace
```

As mentioned above, valid levels are: trace, debug, info, warn, error.

If your setup properly supports `nodetool` (i.e., `scylla-jmx` is running) you can also use a `nodetool` command instead of the aforementioned direct REST API requests. E.g.: `nodetool setlogginglevel sstable trace`.
