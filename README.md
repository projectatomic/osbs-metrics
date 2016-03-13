Some quick-and-dirty tools for getting statistics from an OSBS
instance.

metrics
=======

Use like this:

```
osbs --output=json list-builds > list-builds.json
python ./metrics.py list-builds.json
```

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
