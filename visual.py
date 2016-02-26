from collections import namedtuple
from bokeh.plotting import *
from bokeh.charts import Histogram, TimeSeries
from bokeh.models import Span, NumeralTickFormatter, AdaptiveTicker, Range1d
import numpy as np
import sys


def get_csv(inputfile):
    data = []
    with open(inputfile) as fp:
        for line in fp.readlines():
            data.append(line.rstrip().split(','))

    Metrics = namedtuple('Metrics', data[0])
    return data[0], [Metrics(*row) for row in data[1:]]


def run(metrics, concurrent):
    cfields, cdata = get_csv(concurrent)

    fields, data = get_csv(metrics)
    completion = np.array([row.completion for row in data],
                          dtype=np.datetime64)
    throughput = np.array([row.throughput for row in data], dtype='float64')
    pulp_push = np.array([row.plugin_pulp_push for row in data],
                         dtype='float64')
    upload_size_mb = np.array([row.upload_size_mb for row in data],
                              dtype='float64')

    window_size = 30
    window = np.ones(window_size)/float(window_size)
    throughput_avg = np.convolve(throughput, window, 'same')

    upload_speed = np.divide(upload_size_mb, pulp_push)

    output_file(metrics.replace('.csv', '.html'))

    # hourly throughput
    s1 = figure(width=800, height=350, x_axis_type='datetime',
                title='hourly throughput')
    s1.circle(completion, throughput, color='blue', alpha=0.2, size=12,
              legend='hourly throughput')
    s1.line(completion, throughput_avg, color='navy', legend='throughput (avg)')
    max_throughput = np.max(throughput)
    max_throughput_completion = completion[np.argmax(throughput)]
    peak = Span(location=np.max(throughput), dimension='width',
                line_color='green', line_dash='dashed', line_width=3)
    s1.renderers.extend([peak])

    # upload size / pulp upload time
    s2 = figure(width=800, height=350, title='upload size vs pulp upload time')
    s2.xaxis.axis_label = 'Time uploading to pulp'
    s2.yaxis.axis_label = 'upload size (Mb)'
    s2.xaxis[0].formatter = NumeralTickFormatter(format="00:00:00")
    s2.xaxis[0].ticker = AdaptiveTicker(mantissas=[1,3,6])
    s2.square(pulp_push, upload_size_mb, color='orange', alpha=0.2, size=12)

    # concurrent builds
    concurrent = {
        'timestamp': np.array([row.timestamp for row in cdata],
                              dtype='datetime64'),
        'nbuilds': np.array([row.nbuilds for row in cdata], dtype='int64'),
    }
    s3 = figure(width=800, height=350, title='concurrent builds',
                x_axis_type='datetime')
    s3.line(concurrent['timestamp'],
            concurrent['nbuilds'],
            line_color='green')
    start = Span(location=completion[0].astype('int64') * 1000,
                 dimension='height', name='infra move',
                 line_color='red', line_dash='dashed', line_width=3)
    s3.renderers.extend([start])

    label = 'upload size (Mb)'
    hsize = Histogram({label: upload_size_mb}, values=label,
                      bins=10, plot_width=800, plot_height=350)

    # running time by plugin
    plugin_data = {}
    plugins = {
        'plugin_pull_base_image': 'Time pulling base image',
        'plugin_distgit_fetch_artefacts': 'Time fetching sources',
        'docker_build': 'Time in docker build',
        'plugin_squash': 'Time squashing layers',
        'plugin_pulp_push': 'Time uploading to pulp',
        'running': 'Total build time',
    }
    for plugin, label in plugins.items():
        plugin_data[label] = np.array([getattr(row, plugin)
                                        for row in data], dtype='float64')

    hrunn = Histogram(plugin_data, values=plugins['running'],
                      bins=10, plot_width=800, plot_height=350)
    hrunn._xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    hpull = Histogram(plugin_data, values=plugins['plugin_pull_base_image'],
                      bins=6, plot_width=800, plot_height=350)
    hpull._xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    hfetc = Histogram(plugin_data, values=plugins['plugin_distgit_fetch_artefacts'],
                      bins=6, plot_width=800, plot_height=350)
    hfetc._xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    hbuil = Histogram(plugin_data, values=plugins['docker_build'],
                      bins=10, plot_width=800, plot_height=350)
    hbuil._xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    hsqua = Histogram(plugin_data, values=plugins['plugin_squash'],
                      bins=10, plot_width=800, plot_height=350)
    hsqua._xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    hpulp = Histogram(plugin_data, values=plugins['plugin_pulp_push'],
                      bins=10, plot_width=800, plot_height=350)
    hpulp._xaxis.formatter = NumeralTickFormatter(format="00:00:00")

    p = vplot(s1, s2, s3, hsize, hrunn, hpull, hfetc, hbuil, hsqua, hpulp)
    
    show(p)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
