from collections import namedtuple
from bokeh.plotting import *
import numpy as np
import sys


def get_csv(inputfile):
    data = []
    with open(inputfile) as fp:
        for line in fp.readlines():
            data.append(line.rstrip().split(','))

    Metrics = namedtuple('Metrics', data[0])
    return data[0], [Metrics(*row) for row in data[1:]]


def run(inputfile):
    fields, data = get_csv(inputfile)
    completion = np.array([row.completion for row in data],
                          dtype=np.datetime64)
    throughput = np.array([row.throughput for row in data], dtype='float64')
    pulp_push = np.array([row.pulp_push for row in data], dtype='float64')
    upload_size_mb = np.array([row.upload_size_mb for row in data], dtype='float64')

    window_size = 3
    window = np.ones(window_size)/float(window_size)
    throughput_avg = np.convolve(throughput, window, 'same')

    upload_speed = np.divide(upload_size_mb, pulp_push)

    output_file(inputfile.replace('.csv', '.html'))
    s1 = figure(width=800, height=350, x_axis_type='datetime',
                title='hourly throughput')
    s1.circle(completion, throughput, color='darkgrey',
              legend='hourly throughput')
    s1.line(completion, throughput_avg, color='navy', legend='throughput (avg)')

    s2 = figure(width=800, height=350, x_axis_type='datetime',
                y_range=[0, 1], title='upload size / pulp upload time')
    s2.square(completion, upload_speed, color='orange', legend='upload (Mb/s)')

    p = vplot(s1, s2)
    
    show(p)


if __name__ == '__main__':
    run(sys.argv[1])
