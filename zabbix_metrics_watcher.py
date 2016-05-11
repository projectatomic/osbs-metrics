import argparse
import subprocess
import json
import thread
from time import sleep, time
from tempfile import NamedTemporaryFile


class Build(object):
    def __init__(self, build_name, data=None):
        if not data:
            self.name = build_name
            self._data = {}
            self.load_build_data()
        else:
            self._data = data
            self.name = self._data['metadata']['name']

    def load_build_data(self):
        cmd = ["oc", "get", "build", self.name, "-o", "json"]
        try:
            stdout = subprocess.check_output(cmd)
        except:
            return
        self._data = json.loads(stdout)

    @property
    def state(self):
        return self._data['status']['phase']

    def is_finished(self):
        return self.state in ['Complete', 'Failed', 'Cancelled']

    @property
    def duration(self):
        try:
            return int(self._data['status']['duration'])/1000000000
        except:
            return ""

    @property
    def upload_size_mb(self):
        try:
            tar_metadata = json.loads(self._data['metadata']['annotations']['tar_metadata'])
            return int(tar_metadata['size']) / (1024 * 1024)
        except:
            return 0

    @property
    def durations(self):
        try:
            metadata = json.loads(self._data['metadata']['annotations']['plugins-metadata'])
            return metadata['durations']
        except:
            return {}

    def send_zabbix_notification(self, zabbix_host, osbs_master, concurrent_builds):
        binary_state = 0
        if self.is_finished():
            binary_state = 1

        zabbix_result = {
            'concurrent': concurrent_builds,
            'state': binary_state,
        }
        if self.is_finished():
            for k, v in self.durations.iteritems():
                zabbix_result[k] = v
            zabbix_result['upload_size_mb'] = self.upload_size_mb
            try:
                zabbix_result['pulp_push_speed'] =\
                    self.upload_size_mb / float(zabbix_result['pulp_push'])
            except:
                pass
        zabbix_result['phase'] = self.state
        zabbix_result['name'] = self.name
        print(zabbix_result)

        # First send the real data for the build
        with NamedTemporaryFile(delete=True) as temp_zabbix_data:
            for k, v in zabbix_result.iteritems():
                temp_zabbix_data.write("- %s %s\n" % (k, v))
            temp_zabbix_data.flush()

            cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -i "%s"' % (
                  zabbix_host, osbs_master, temp_zabbix_data.name)
            print("running %s:" % cmd)
            try:
                print(subprocess.check_output(cmd, shell=True))
            except:
                pass

        sleep(10)
        # Now we need to send zeros so the data from previous run won't pollute next runs
        with NamedTemporaryFile(delete=True) as temp_zabbix_data:
            for k, v in zabbix_result.iteritems():
                if k not in ['concurrent', 'pulp_push_speed']:
                    temp_zabbix_data.write("- %s 0\n" % k)
            temp_zabbix_data.flush()

            cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -i "%s"' % (
                  zabbix_host, osbs_master, temp_zabbix_data.name)
            print("running %s:" % cmd)
            try:
                print(subprocess.check_output(cmd, shell=True))
            except:
                pass


def _send_zabbix_message(zabbix_host, osbs_master, key, value, print_command=True):
    cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -k %s -o "%s"' % (
          zabbix_host, osbs_master, key, value)
    if print_command:
        print(cmd)
    try:
        subprocess.check_output(cmd, shell=True)
    except:
        pass


def filter_completed_builds(completed_builds):
    # Remove all completed_builds which are not within this hour
    now = int(time())
    return {k: v for k, v in completed_builds.items() if (v - now) < 3600}


def heartbeat(zabbix_host, osbs_master):
    while True:
        _send_zabbix_message(zabbix_host, osbs_master,
                             "heartbeat", int(time()), print_command=False)
        sleep(10)


def run(zabbix_host, osbs_master):
    running_builds = set()
    pending = {}
    completed_builds = {}

    thread.start_new_thread(heartbeat, (zabbix_host, osbs_master, ))

    while True:
        cmd = ["oc", "get", "builds", "--watch-only", "--no-headers=true"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, ''):
            line = ' '.join(line.split())
            print(line)
            line_arr = line.strip().split(' ')
            if len(line_arr) < 4:
                continue

            build_name = line_arr[0]
            status = line_arr[3]
            if status in ['Pending', 'Running', 'Complete', 'Failed', 'Cancelled'] or \
                    'Error' in status:
                if status == 'Pending':
                    pending[build_name] = int(time())
                elif status == 'Running':
                    running_builds.add(build_name)
                    if build_name in pending.keys():
                        pending_duration = int(time()) - pending[build_name]
                        _send_zabbix_message(zabbix_host, osbs_master, "pending", pending_duration)
                        del pending[build_name]
                else:
                    try:
                        running_builds.remove(build_name)
                        if status != 'Cancelled':
                            completed_builds[build_name] = int(time())
                            completed_builds = filter_completed_builds(completed_builds)
                            _send_zabbix_message(zabbix_host, osbs_master,
                                                 "throughput", len(completed_builds))
                    except:
                        pass

                build = Build(build_name)
                build.send_zabbix_notification(zabbix_host, osbs_master, len(running_builds))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--zabbix-host")
    parser.add_argument("--osbs-master")
    args = parser.parse_args()

    run(args.zabbix_host, args.osbs_master)
