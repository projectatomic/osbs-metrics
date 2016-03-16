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
