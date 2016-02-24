from calendar import timegm
from collections import defaultdict, namedtuple
import json
import os
import re
import subprocess
import sys
from time import ctime, gmtime, strftime, strptime


FIELDS = ['completion',
          'throughput',
          'pending',
          'running',
          'pull_base_image',
          'distgit_fetch_artefacts',
          'squash',
          'pulp_push',
          'upload_size_mb']
Metrics = namedtuple('Metrics', FIELDS)


def rfc3339_time(rfc3339):
    time_tuple = strptime(rfc3339, '%Y-%m-%dT%H:%M:%SZ')
    return timegm(time_tuple)


class ThroughputModel(object):
    def __init__(self, window):
        self.builds = []
        self.start_time = None  # start of window
        self.window = window

    def append(self, timestamp):
        if not self.builds:
            self.start_time = timestamp

        self.builds.append(timestamp)
        while self.builds[-1] - self.builds[0] > self.window:
            del self.builds[0]

        return len(self.builds)
        

class BuildLog(object):
    def __init__(self, logfile):
        self.logfile = logfile
        self.data = None
        self._trawl()

    def _trawl(self):
        if self.data is not None:
            return

        self.data = {
            'upload_size_mb': '-',
        }
        size_re = re.compile(r' - dockpulp - INFO - uploading a (.*)M image')
        plugin_re = re.compile(r'(.*),[0-9]+ - atomic_reactor.plugin - DEBUG - running plugin \'(.*)\'')
        with open(self.logfile) as lf:
            log = lf.read()
            size = size_re.search(log)
            if size:
                self.data['upload_size_mb'] = size.groups()[0]

            last_plugin = None
            while True:
                plugin = plugin_re.search(log)
                if plugin is None:
                    break

                timestamp, plugin_name = plugin.groups()
                t = timegm(strptime(timestamp, "%Y-%m-%d %H:%M:%S"))
                if last_plugin is not None:
                    self.data[last_plugin[1]] = t - last_plugin[0]

                last_plugin = (t, plugin_name)
                log = log[plugin.span()[1]:]


class Builds(object):
    def __init__(self, builds):
        self.builds = builds

    def get_stats(self):
        builds_examined = 0
        earliest_completion = None
        latest_completion = None
        states = defaultdict(int)
        tputmodel = ThroughputModel(60 * 60)
        results = {
            'archived': [],
            'current': [],
        }

        # Sort by time created
        builds = [build for build in self.builds
                  if 'completionTimestamp' in build['status']]
        builds.sort(key=lambda x: x['status']['completionTimestamp'])

        for build in builds:
            completionTimestamp = build['status']['completionTimestamp']
            completion = rfc3339_time(completionTimestamp)
            if earliest_completion is None:
                earliest_completion = latest_completion = completion

            latest_completion = completion

            state = build['status']['phase']
            states[state] += 1
            if state == 'Complete':
                timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime(completion))
                tput = tputmodel.append(completion)

                creationTimestamp = build['metadata']['creationTimestamp']
                creation = rfc3339_time(creationTimestamp)
                startTimestamp = build['status']['startTimestamp']
                start = rfc3339_time(startTimestamp)
                name = build['metadata']['name']
                pending = start - creation
                plugins = {name: '-'
                           for name in ['pull_base_image',
                                        'distgit_fetch_artefacts',
                                        'squash',
                                        'pulp_push']}
                if pending < 0:
                    which = 'archived'
                    pending = upload_size_mb = '-'
                else:
                    which = 'current'
                    logfile = "{name}.log".format(name=name)
                    if not os.access(logfile, os.R_OK):
                        cmd = ['osbs',
                               'build-logs',
                               name]
                        with open(logfile, 'w') as fp:
                            print(' '.join(cmd))
                            p = subprocess.Popen(cmd, stdout=fp)
                            p.communicate()

                    build_log = BuildLog(logfile)
                    log_data = build_log.data
                    upload_size_mb = log_data.get('upload_size_mb', '-')
                    for plugin in plugins.keys():
                        try:
                            plugins[plugin] = log_data[plugin]
                        except KeyError:
                            pass

                metrics = Metrics(completion=timestamp,
                                  throughput=tput,
                                  pending=pending,
                                  running=completion - start,
                                  upload_size_mb=upload_size_mb,
                                  **plugins)
                results[which].append(metrics)

            builds_examined += 1

        for which, data in results.items():
            with open("metrics-{which}.csv".format(which=which), "w") as fp:
                fp.write(",".join(FIELDS) + '\n')
                for result in data:
                    fp.write(",".join([str(m) for m in result]) + '\n')

        return {
            'builds examined': builds_examined,
            'earliest_completion': ctime(earliest_completion),
            'latest_completion': ctime(latest_completion),
            'states': states,
        }

        
def run(inputfile):
    with open(inputfile) as fp:
        builds = json.load(fp)

    print(json.dumps(Builds(builds).get_stats(), sort_keys=True, indent=2))


if __name__ == '__main__':
    run(sys.argv[1])
