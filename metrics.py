from collections import defaultdict, namedtuple
import json
import sys
from time import ctime, gmtime, strftime, strptime
from calendar import timegm


FIELDS = ['completion', 'throughput', 'pending', 'running']
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
        
        
class Builds(object):
    def __init__(self, builds):
        self.builds = builds

    def get_stats(self):
        builds_examined = 0
        earliest_completion = None
        latest_completion = None
        states = defaultdict(int)
        tputmodel = ThroughputModel(60 * 60)
        results = []

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
                pending = start - creation
                if pending < 0:
                    pending = '-'

                metrics = Metrics(completion=timestamp,
                                  throughput=tput,
                                  pending=pending,
                                  running=completion - start)
                results.append(metrics)

            builds_examined += 1

        with open("throughput.csv", "w") as fp:
            fp.write(",".join(FIELDS) + '\n')
            for result in results:
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
