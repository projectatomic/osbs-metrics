from bokeh.plotting import *
from bokeh.charts import Histogram, TimeSeries
from bokeh.models import Span, NumeralTickFormatter, AdaptiveTicker, Range1d
from collections import namedtuple
import datetime
import numpy as np
import pandas as pd
import sys


def MyHistogram(data, bins, **kwargs):
    # Work around bokeh.charts.Histogram bug
    p = figure(**kwargs)
    hist, edges = np.histogram(data, density=True, bins=bins)
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],
           color='#dd2222', line_color='black')
    return p


def run(metrics_file, concurrent_file):
    concurrent = pd.read_csv(concurrent_file, parse_dates=['timestamp'])

    metrics = pd.read_csv(metrics_file, parse_dates=['completion'])

    # Network fix made 2016-02-26 approx 2000 UTC
    network_fix = metrics['completion'] > datetime.datetime(2016, 2, 26,
                                                            20, 0)

    window_size = 30
    window = np.ones(window_size)/float(window_size)
    throughput_avg = np.convolve(metrics['throughput'], window, 'same')

    output_file(metrics_file.replace('.csv', '.html'))
    charts = []

    # hourly throughput
    s1 = figure(width=800, height=350, x_axis_type='datetime',
                title='hourly throughput')
    s1.circle(metrics['completion'],
              metrics['throughput'],
              color='blue', alpha=0.2, size=12,
              legend='hourly throughput')
    s1.line(metrics['completion'],
            throughput_avg,
            color='navy', legend='throughput (avg)')
    peak = Span(location=metrics['throughput'].max(), dimension='width',
                line_color='green', line_dash='dashed', line_width=3)
    s1.renderers.extend([peak])
    charts.append(s1)

    # upload size / pulp upload time
    s2b = figure(width=800, height=350,
                 title='upload size vs pulp upload time (to Feb 26)')
    s2b.xaxis.axis_label = 'Time uploading to pulp'
    s2b.yaxis.axis_label = 'upload size (Mb)'
    s2b.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    s2b.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
    s2b.square(metrics['plugin_pulp_push'][~network_fix],
               metrics['upload_size_mb'][~network_fix],
               color='orange', alpha=0.2, size=12)
    charts.append(s2b)

    s2a = figure(width=800, height=350,
                 title='upload size vs pulp upload time (since Feb 26)')
    s2a.xaxis.axis_label = 'Time uploading to pulp'
    s2a.yaxis.axis_label = 'upload size (Mb)'
    s2a.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    s2a.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
    s2a.square(metrics['plugin_pulp_push'][network_fix],
               metrics['upload_size_mb'][network_fix],
               color='orange', alpha=0.2, size=12)
    charts.append(s2a)

    # concurrent builds
    s3 = figure(width=800, height=350, title='concurrent builds',
                x_axis_type='datetime')
    s3.line(concurrent['timestamp'],
            concurrent['nbuilds'],
            line_color='green',
            line_join='bevel')
    start = Span(location=metrics['completion'][0].timestamp() * 1000,
                 dimension='height', name='infra move',
                 line_color='red', line_dash='dashed', line_width=3)
    s3.renderers.extend([start])
    charts.append(s3)

    valid = ~np.isnan(metrics['upload_size_mb'])
    hsize = MyHistogram(metrics['upload_size_mb'][valid], bins=10,
                        title='Upload size',
                        plot_width=800, plot_height=350)
    hsize.xaxis.axis_label = 'Mb'
    charts.append(hsize)

    # running time by plugin
    for values, bins, title in [
            (metrics[valid]['running'], None,
             'Total build time'),

            (metrics[valid]['plugin_pull_base_image'], 6,
             'Time pulling base image'),

            (metrics[valid]['plugin_distgit_fetch_artefacts'], 6,
             'Time fetching sources'),

            (metrics[valid]['docker_build'], None,
             'Time in docker build'),

            (metrics[valid]['plugin_squash'], None,
             'Time squashing layers'),

            (metrics[valid]['plugin_pulp_push'][~network_fix], None,
             'Time uploading to pulp (to Feb 26)'),

            (metrics[valid]['plugin_pulp_push'][network_fix], None,
             'Time uploading to pulp (since Feb 26)'),
    ]:
        h = MyHistogram(values, title=title, x_axis_type='datetime',
                        bins=bins or 10, plot_width=800, plot_height=350)
        h.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
        h.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
        h.yaxis.bounds = (0, len(metrics[valid]))
        charts.append(h)

    p = vplot(*charts)
    show(p)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
