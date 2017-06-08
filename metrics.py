from calendar import timegm
from collections import defaultdict, namedtuple
import json
import os
import re
import subprocess
import sys
import argparse
from time import ctime, gmtime, strftime, strptime


FIELDS = [('name', 'name'),
          ('image', 'image'),
          ('completion', 'completion'),
          ('state', 'state'),
          ('throughput', 'throughput'),
          ('pending', 'pending'),
          ('running', 'running'),
          ('pull_base_image', 'plugin_pull_base_image'),
          ('distgit_fetch_artefacts', 'plugin_distgit_fetch_artefacts'),
          ('dockerfile_content', 'docker_build'),
          ('squash', 'plugin_squash'),
          ('compress', 'plugin_compress'),
          ('pulp_push', 'plugin_pulp_push'),
          ('upload_size_mb', 'upload_size_mb'),
          ('failed_plugin', 'failed_plugin'),
          ('exception', 'exception')]
Metrics = namedtuple('Metrics', [field[0] for field in FIELDS])


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
        while self.builds[-1] - self.builds[0] >= self.window:
            del self.builds[0]

        return len(self.builds)


class ConcurrentModel(object):
    def __init__(self):
        self.start_finish = []
        self.finish_times = []

    def append(self, start, finish):
        self.start_finish.append((start, finish))

    def get_nbuilds(self):
        for start, finish in self.start_finish:
            while self.finish_times:
                if start < self.finish_times[0]:
                    break

                yield (self.finish_times[0], len(self.finish_times) - 1)
                del self.finish_times[0]

            self.finish_times.append(finish)
            self.finish_times.sort()
            yield (start, len(self.finish_times))


class MissingLog(Exception):
    pass


class Builds(object):
    def __init__(self, builds, osbs_instance=None):
        self.osbs_instance = osbs_instance
        self.builds = builds

    def get_stats(self):
        builds_examined = 0
        earliest_completion = None
        latest_completion = None
        states = defaultdict(int)
        tputmodel = ThroughputModel(60 * 60)
        missing = []
        results = {
            'archived': [],
            'current': [],
            'concurrent': [],
        }

        # Sort by time created
        builds = [build for build in self.builds
                  if 'completionTimestamp' in build['status']]
        builds.sort(key=lambda x: x['status']['completionTimestamp'])

        tput = 0
        for build in builds:
            upload_size_mb = 'nan'
            name = build['metadata']['name']
            completionTimestamp = build['status']['completionTimestamp']
            completion = rfc3339_time(completionTimestamp)
            timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime(completion))
            if earliest_completion is None:
                earliest_completion = latest_completion = completion

            latest_completion = completion

            state = build['status']['phase']
            states[state] += 1
            creationTimestamp = build['metadata']['creationTimestamp']
            creation = rfc3339_time(creationTimestamp)
            startTimestamp = build['status'].get('startTimestamp')
            if startTimestamp is None:
                continue

            start = rfc3339_time(startTimestamp)
            pending = start - creation
            if pending < 0:
                which = 'archived'
                pending = 'nan'
            else:
                which = 'current'

            duration = build['status'].get('duration', 0) / 1000000000
            plugins = {name: 'nan'
                       for name in ['pull_base_image',
                                    'distgit_fetch_artefacts',
                                    'dockerfile_content',
                                    'squash',
                                    'compress',
                                    'pulp_push',
                                    'image',
                                    'failed_plugin',
                                    'exception']}

            try:
                plugins_metadata = json.loads(build['metadata']['annotations']['plugins-metadata'])
                durations = plugins_metadata['durations']
            except Exception:
                plugins_metadata = {}
                durations = {}

            try:
                errors = plugins_metadata['errors']
                first_failed = sorted(errors.keys())[0]
                plugins['failed_plugin'] = first_failed
                exception_text = errors[first_failed].split("(")[0]
                # Make sure commas are escaped and double quotes are replaced
                plugins['exception'] = json.dumps(exception_text.replace('"', "'"))
            except (KeyError, IndexError):
                pass

            if state == 'Complete':
                # Count this towards throughput
                tput = tputmodel.append(completion)

                if which == 'current':
                    annotations = build['metadata'].get('annotations', {})
                    tar_metadata = annotations.get('tar_metadata')
                    if tar_metadata:
                        md = json.loads(tar_metadata)
                        upload_size_mb = md['size'] / (1024 * 1024)

                    for plugin in plugins.keys():
                        try:
                            plugins[plugin] = durations[plugin]
                        except KeyError:
                            pass
                        else:
                            continue

                    repositories = annotations.get('repositories')
                    if repositories:
                        repositories_json = json.loads(repositories)
                        try:
                            image_name = '/'.join(repositories_json['unique'][0].split('/')[1:])
                            plugins['image'] = image_name.split(':')[0]
                        except IndexError:
                            plugins['image'] = ''

                metrics = Metrics(name=name,
                                  completion=timestamp,
                                  state=state,
                                  throughput=tput,
                                  pending=pending,
                                  running=duration,
                                  upload_size_mb=upload_size_mb,
                                  **plugins)
                results[which].append(metrics)
            elif state == 'Failed':
                metrics = Metrics(name=name,
                                  completion=timestamp,
                                  state=state,
                                  throughput=tput,
                                  pending=pending,
                                  running=duration,
                                  upload_size_mb='nan',
                                  **plugins)
                results[which].append(metrics)

            builds_examined += 1

        # Now sort by time started
        builds = [build for build in self.builds
                  if ('startTimestamp' in build['status'] and
                      'completionTimestamp' in build['status'])]
        builds.sort(key=lambda x: x['status']['startTimestamp'])
        cmodel = ConcurrentModel()
        for build in builds:
            startTimestamp = build['status']['startTimestamp']
            start = rfc3339_time(startTimestamp)
            completionTimestamp = build['status']['completionTimestamp']
            completion = rfc3339_time(completionTimestamp)
            cmodel.append(start, completion)

        results['concurrent'].extend(
                [(strftime("%Y-%m-%d %H:%M:%S", gmtime(timestamp)), nbuilds)
                 for (timestamp, nbuilds) in cmodel.get_nbuilds()])

        for which, data in results.items():
            if which == 'concurrent':
                with open("metrics-concurrent.csv", "w") as fp:
                    fp.write("timestamp,nbuilds\n")
                    for result in data:
                        fp.write(",".join([str(m) for m in result]) + '\n')
            else:
                with open("metrics-{which}.csv".format(which=which), "w") as fp:
                    fp.write(",".join([field[1] for field in FIELDS]) + '\n')
                    for result in data:
                        fp.write(",".join([str(m) for m in result]) + '\n')

        return {
            'builds examined': builds_examined,
            'earliest_completion': ctime(earliest_completion),
            'latest_completion': ctime(latest_completion),
            'states': states,
            'missing-log': missing,
        }

        
def run(inputfile=None, instance=None):
    if inputfile is not None:
        with open(inputfile) as fp:
            builds = json.load(fp)
    else:
        builds = json.load(sys.stdin)

    print(json.dumps(Builds(builds, instance).get_stats(), sort_keys=True, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance")
    parser.add_argument("inputfile", nargs='?', default=None)
    args = parser.parse_args()

    run(args.inputfile, args.instance)
