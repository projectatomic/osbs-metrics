Some quick-and-dirty tools for getting statistics from an OSBS
instance.

metrics
=======

Use like this:

```
osbs --output=json list-builds > list-builds.json
python ./metrics.py list-builds.json
```

If you want to skip metrics that require logs (plugin-level timings
and failure reasons), set METRICS_REQUIRE_LOGS=0 in the environment.

visual
======

After running metrics, use like this:

```
python ./visual metrics-current.csv metrics-concurrent.csv
```

graph
=====

Use like this:

```
osbs --output=json list-builds | \
    python ./graph.py | \
    graph-easy --as=dot | \
    dot -Tsvg > builds.svg
```

```graph.py``` also has optional parameters:

  * inputfile: read builds json from file
  * pulp_base_url: use pulp API to get layer size
      (should be used in case ```tar_metadata``` is inconsistent)

zabbix
=====

Send build metadata to Zabbix synthetic host:

```
python zabbix_metrics_watcher.py \
    --zabbix-host <zabbix host> \
    --osbs-master <synthetic OSBS host on zabbix> \
    --instance <instance name in osbs config>
```

For outdated metadata (atomic-reactor < 1.6.4) ```zabbix_metrics_watcher_oldmetadata.py```
should be used
