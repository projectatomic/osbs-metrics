from bokeh.plotting import *
from bokeh.charts import Histogram, TimeSeries, BoxPlot
from bokeh.models import Span, NumeralTickFormatter, AdaptiveTicker, Range1d
from collections import namedtuple
import datetime
import numpy as np
import pandas as pd
import sys


def MyHistogram(data, bins, **kwargs):
    # Work around bokeh.charts.Histogram bug
    # https://github.com/bokeh/bokeh/issues/3875
    p = figure(**kwargs)
    hist, edges = np.histogram(data, density=False, bins=bins)
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],
           color='#dd2222', line_color='black')
    return p


def run(metrics_file, concurrent_file, postfeb26):
    concurrent = pd.read_csv(concurrent_file, parse_dates=['timestamp'])

    metrics = pd.read_csv(metrics_file, parse_dates=['completion'])

    # Network fix made 2016-02-26 approx 2000 UTC
    network_fix = metrics['completion'] > datetime.datetime(2016, 2, 26,
                                                            20, 0)
    if postfeb26:
        fname = '-since-feb26'
        when = ' (since Feb 26)'
        which = network_fix
    else:
        fname = '-to-feb26'
        when = ' (to Feb 26)'
        which = ~network_fix

    output_file(metrics_file.replace('.csv', fname + '.html'))
    charts = []

    # hourly throughput
    s1 = figure(width=800, height=350, x_axis_type='datetime',
                title='hourly throughput' + when)
    s1.legend.orientation = 'bottom_left'
    s1.circle(metrics[which]['completion'],
              metrics[which]['throughput'],
              color='blue', alpha=0.2, size=12,
              legend='hourly throughput')
    peak = Span(location=metrics[which]['throughput'].max(), dimension='width',
                line_color='green', line_dash='dashed', line_width=3)
    s1.renderers.extend([peak])
    charts.append(s1)

    # upload size / pulp upload time
    s2 = figure(width=800, height=350,
                title='upload size vs pulp upload time' + when)
    s2.xaxis.axis_label = 'Time uploading to pulp'
    s2.yaxis.axis_label = 'upload size (Mb)'
    s2.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    s2.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
    s2.square(metrics[which]['plugin_pulp_push'],
              metrics[which]['upload_size_mb'],
              color='orange', alpha=0.2, size=12)
    charts.append(s2)

    # concurrent builds
    s3 = figure(width=800, height=350, title='concurrent builds' + when,
                x_axis_type='datetime')
    start = metrics['completion'][0]
    which_c = concurrent['timestamp'] > start
    if not postfeb26:
        which_c = ~which_c

    s3.line(concurrent[which_c]['timestamp'],
            concurrent[which_c]['nbuilds'],
            line_color='green',
            line_join='bevel')
    charts.append(s3)

    merged = metrics.merge(concurrent,
                           left_on=['completion'], right_on=['timestamp'],
                           sort=False)
    sc = BoxPlot(merged, values='plugin_squash', label='nbuilds',
                 width=800, height=350,
                 title='squash time vs (other) concurrent builds (all time)')
    sc._yaxis.formatter = NumeralTickFormatter(format="00:00:00")
    sc._yaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
    charts.append(sc)

    valid = ~np.isnan(metrics['upload_size_mb'])
    hsize = MyHistogram(metrics['upload_size_mb'][which][valid], bins=10,
                        title='Upload size' + when,
                        plot_width=800, plot_height=350)
    hsize.xaxis.axis_label = 'Mb'
    charts.append(hsize)

    # running time by plugin
    these_metrics = metrics[which][valid]
    for values, bins, title in [
            (these_metrics['running'], None,
             'Total build time' + when),

            (these_metrics['plugin_pull_base_image'], 15,
             'Time pulling base image' + when),

            (these_metrics['plugin_distgit_fetch_artefacts'], 6,
             'Time fetching sources' + when),

            (these_metrics['docker_build'], None,
             'Time in docker build' + when),

            (these_metrics['plugin_squash'], None,
             'Time squashing layers' + when),

            (these_metrics['plugin_pulp_push'], None,
             'Time uploading to pulp' + when),
    ]:
        h = MyHistogram(values, title=title, x_axis_type='datetime',
                        bins=bins or 10, plot_width=800, plot_height=350)
        h.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
        h.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
        h.yaxis.bounds = (0, len(these_metrics))
        charts.append(h)

    p = vplot(*charts)
    show(p)


if __name__ == '__main__':
    for postfeb26 in False, True:
        run(sys.argv[1], sys.argv[2], postfeb26)
