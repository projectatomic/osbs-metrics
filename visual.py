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


class Charts(object):
    def __init__(self, metrics_file, concurrent_file):
        self.concurrent = pd.read_csv(concurrent_file,
                                      parse_dates=['timestamp'])
        self.all_metrics = pd.read_csv(metrics_file,
                                       parse_dates=['completion'],
                                       na_values={'image': ''},
                                       keep_default_na=False)
        self.completed = self.all_metrics['state'] == 'Complete'
        self.metrics = self.all_metrics[self.completed]

        # Work out which image has median compressed size
        compressed = ~np.isnan(self.metrics['plugin_compress'])
        has_image = self.metrics['image'] != ''
        median = np.median(self.metrics[compressed & has_image]['upload_size_mb'])
        match = self.metrics[has_image &
                             (np.abs(self.metrics['upload_size_mb'] -
                                     median) < 1)]['image']
        self.image = match.values[0]

    def get_time_charts(self, time_selector, suffix, width=600, height=350):
        charts = []

        selector = time_selector(self.metrics['completion'])

        # hourly throughput
        s1 = figure(width=width, height=height, x_axis_type='datetime',
                    title='hourly throughput' + suffix)
        s1.legend.orientation = 'bottom_left'
        s1.circle(self.metrics[selector & self.completed]['completion'],
                  self.metrics[selector & self.completed]['throughput'],
                  color='blue', alpha=0.2, size=12,
                  legend='hourly throughput')
        peak = Span(location=self.metrics[selector]['throughput'].max(),
                    dimension='width',
                    line_color='green', line_dash='dashed', line_width=3)
        s1.renderers.extend([peak])
        charts.append(s1)

        # upload size / pulp upload time
        s2 = figure(width=width, height=height,
                    title='upload size vs pulp upload time' + suffix)
        s2.xaxis.axis_label = 'Time uploading to pulp'
        s2.yaxis.axis_label = 'upload size (Mb)'
        s2.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
        s2.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
        s2.square(self.metrics[selector]['plugin_pulp_push'],
                  self.metrics[selector]['upload_size_mb'],
                  color='orange', alpha=0.2, size=12)
        charts.append(s2)

        # concurrent builds
        s3 = figure(width=width, height=height, title='concurrent builds' + suffix,
                    x_axis_type='datetime')
        which_c = time_selector(self.concurrent['timestamp'])
        s3.line(self.concurrent[which_c]['timestamp'],
                self.concurrent[which_c]['nbuilds'],
                line_color='green',
                line_join='bevel')
        charts.append(s3)

        # squash time vs concurrent builds
        merged = self.metrics[selector].merge(self.concurrent[which_c],
                                              left_on=['completion'],
                                              right_on=['timestamp'],
                                              sort=False)
        sc = BoxPlot(merged, values='plugin_squash', label='nbuilds',
                     width=width, height=height,
                     title='squash time vs (other) concurrent builds' + suffix)
        sc._yaxis.formatter = NumeralTickFormatter(format="00:00:00")
        sc._yaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
        charts.append(sc)

        # upload_size_mb
        valid = ~np.isnan(self.metrics['upload_size_mb'])
        hsize = MyHistogram(self.metrics['upload_size_mb'][selector][valid], bins=10,
                            title='Upload size' + suffix,
                            plot_width=width, plot_height=height)
        hsize.xaxis.axis_label = 'Mb'
        charts.append(hsize)

        # running time by plugin
        these_metrics = self.metrics[selector]
        for column, bins, title in [
            ('running', None,
             'Total build time' + suffix),

            ('plugin_pull_base_image', 15,
             'Time pulling base image' + suffix),

            ('plugin_distgit_fetch_artefacts', None,
             'Time fetching sources' + suffix),

            ('docker_build', None,
             'Time in docker build' + suffix),

            ('plugin_squash', None,
             'Time squashing layers' + suffix),

            ('plugin_pulp_push', None,
             'Time uploading to pulp' + suffix),
        ]:
            values = these_metrics[column][~np.isnan(these_metrics[column])]
            h = MyHistogram(values, title=title, x_axis_type='datetime',
                            bins=bins or 10, plot_width=width, plot_height=height)
            h.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
            h.xaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
            h.yaxis.bounds = (0, len(these_metrics))
            charts.append(h)

        # Now show plugin-level timings for a specific image
        # data looks like:
        # completion  image       plugin_x  plugin_y
        # 2016-03-18  image/name    205       60
        #
        # reshape to:
        # imgae       plugin      value
        # image/name  plugin_x    205
        # image/name  plugin_y    60
        is_image = self.metrics[selector]['image'] == self.image
        image = self.metrics[selector][is_image]
        timings = pd.melt(image[['image',
                                 'running',
                                 'plugin_pull_base_image',
                                 'plugin_distgit_fetch_artefacts',
                                 'docker_build',
                                 'plugin_squash',
                                 'plugin_compress',
                                 'plugin_pulp_push']],
                          id_vars=['image'], var_name='plugin')
        im = BoxPlot(timings, values='value', label='plugin',
                     width=width, height=height * 2,
                     title='%s timings%s' % (self.image, suffix))
        im._yaxis.formatter = NumeralTickFormatter(format="00:00:00")
        im._yaxis.ticker = AdaptiveTicker(mantissas=[1,3,6])
        charts.append(im)

        return charts

    def run(self):
        def since(x):
            return x > datetime.date(2016, 3, 16)

        def until(x):
            return ((x > datetime.datetime(2016, 2, 26, 20, 0)) &
                    (x <= datetime.date(2016, 3, 16)))

        time_charts = [self.get_time_charts(until, ' (Feb 26 - Mar 16)'),
                       self.get_time_charts(since, ' (since Mar 16)')]
        p = [hplot(*x) for x in zip(*time_charts)]
        charts = vplot(*p)
        output_file('metrics.html')
        show(charts)


if __name__ == '__main__':
    Charts(sys.argv[1], sys.argv[2]).run()
