import argparse
import subprocess
import json
import logging
import dateutil.parser
import datetime
from dateutil.tz import tzutc
from time import sleep
from tempfile import NamedTemporaryFile

logger = logging.getLogger('osbs-metrics')
logger.handlers = []
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

"""
Check the output of 'osbs watch-builds' and send messages to zabbix

Zabbix Items (generally sent when a build changes a state):
 * new_duration - maximum time any current build spent in New state
 * pending - amount of time last build has spent in Pending state
 * throughput - number of build which successfully completed during last hour
 * concurrent - number of running builds when some build has changed state
 * name - build name
 * phase - name of the phase this build is in
 * state - 0 for any in progress state, 1 for complete (Complete / Failed / Cancelled)
 * <atomic-reactor plugin name> - how long did each plugin took to complete
 * pulp_push_speed - average speed of pulp push (or pulp_sync if pulp_push wasn't performed)
 * upload_size_mb - size of the image uploaded to koji

The data are stored in a plain table and can be linked together by a unique timestamp only
"""


class Build(object):
    def __init__(self, build_name, cmd_base, data=None):
        logger.info("Creating build %s:%s", build_name, data)
        self.cmd_base = cmd_base
        if not data:
            self.name = build_name
            self._data = {}
            self.load_build_data()
        else:
            self._data = data
            self.name = self._data['metadata']['name']

    def load_build_data(self):
        cmd = self.cmd_base + ["get-build", self.name]
        try:
            stdout = subprocess.check_output(cmd)
            self._data = json.loads(stdout)
            logger.info("build data loaded")
        except subprocess.CalledProcessError as e:
            logger.warn("Error while fetching build data: %r", e)
            logger.warn('Exit code: %s', e.returncode)
            logger.warn('Output: %s',  e.output)

    @property
    def state(self):
        return self._data['status']['phase']

    def is_finished(self):
        return self.state in ['Complete', 'Failed', 'Cancelled']

    @property
    def duration(self):
        try:
            return int(self._data['status']['duration'])/1000000000
        except Exception as e:
            logger.warn('Error duration: %r', e)
            return ""

    @property
    def upload_size_mb(self):
        try:
            tar_metadata = json.loads(self._data['metadata']['annotations']['tar_metadata'])
            return int(tar_metadata['size']) / (1024 * 1024)
        except Exception as e:
            logger.warn('Error upload_size_mb: %r', e)
            return 0

    @property
    def durations(self):
        try:
            metadata = json.loads(self._data['metadata']['annotations']['plugins-metadata'])
            return metadata['durations']
        except Exception as e:
            logger.warn('Error durations: %r', e)
            return {}

    @property
    def created_time(self):
        try:
            timestamp = self._data['metadata']['creationTimestamp']
            return dateutil.parser.parse(timestamp)
        except Exception as e:
            logger.warn('Error created_time: %r', e)
            return None

    @property
    def started_time(self):
        try:
            timestamp = self._data['status']['startTimestamp']
            return dateutil.parser.parse(timestamp)
        except Exception as e:
            logger.warn('Error started_time: %r', e)
            return None

    @property
    def completed_time(self):
        try:
            timestamp = self._data['status']['completionTimestamp']
            return dateutil.parser.parse(timestamp)
        except Exception as e:
            logger.warn('Error completed_time: %r', e)
            return None

    def send_zabbix_notification(self, zabbix_host, osbs_master, concurrent_builds):
        logger.info("Sending zabbix notification for build %s", self.name)
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
                if 'pulp_push' in zabbix_result.keys():
                    zabbix_result['pulp_push_speed'] =\
                        self.upload_size_mb / float(zabbix_result['pulp_push'])
                elif 'pulp_sync' in zabbix_result.keys():
                    zabbix_result['pulp_push_speed'] =\
                        self.upload_size_mb / float(zabbix_result['pulp_sync'])
            except Exception as e:
                logger.warn('Error calculating push speed: %s', e)
        zabbix_result['phase'] = self.state
        zabbix_result['name'] = self.name
        logger.info("Notification %s ", zabbix_result)

        # First send the real data for the build
        with NamedTemporaryFile(delete=True) as temp_zabbix_data:
            for k, v in zabbix_result.iteritems():
                temp_zabbix_data.write("- %s %s\n" % (k, v))
            temp_zabbix_data.flush()

            cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -i "%s"' % (
                  zabbix_host, osbs_master, temp_zabbix_data.name)
            logger.info("Sending build data: %s", cmd)
            try:
                output = subprocess.check_output(cmd, shell=True)
                logger.info('Output:\n%s', output)
            except subprocess.CalledProcessError as e:
                logger.warn('Error while sending build data: %s', e)
                logger.warn('Exit code: %s', e.returncode)
                logger.warn('Output: %s', e.output)

        sleep(1)
        # Now we need to send zeros so the data from previous run won't pollute next runs
        logger.info("Sending zero data")
        with NamedTemporaryFile(delete=True) as temp_zabbix_data:
            for k, v in zabbix_result.iteritems():
                if k not in ['concurrent', 'pulp_push_speed', 'name', 'phase', 'failed_plugin', 'exception']:
                    temp_zabbix_data.write("- %s 0\n" % k)
            temp_zabbix_data.flush()

            cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -i "%s"' % (
                  zabbix_host, osbs_master, temp_zabbix_data.name)
            try:
                output = subprocess.check_output(cmd, shell=True)
                logger.info('Output:\n%s', output)
            except subprocess.CalledProcessError as e:
                logger.warn('Error while sending build data: %r', e)
                logger.warn('Exit code: %s', e.returncode)
                logger.warn('Output: %s', e.output)


def _send_zabbix_message(zabbix_host, osbs_master, key, value, print_command=True):
    cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -k %s -o "%s"' % (
          zabbix_host, osbs_master, key, value)
    if print_command:
        logger.info("running %s:", cmd)
    try:
        output = subprocess.check_output(cmd, shell=True)
        if print_command:
            logger.info('Output:\n%s', output)
    except subprocess.CalledProcessError as e:
        logger.warn('Error while sending zabbix message: %r', e)
        logger.warn('Exit code: %s', e.returncode)
        logger.warn('Output: %s', e.output)


def filter_completed_builds(completed_builds):
    # Remove all completed_builds which are not within this hour
    return {k: v for k, v in completed_builds.items()
            if (datetime.datetime.now(tzutc()) - v).total_seconds() < 3600}


def run(zabbix_host, osbs_master, config, instance):
    running_builds = set()
    builds_in_new = {}
    pending = set()
    completed_builds = {}

    cmd_base = ["osbs", "--output", "json"]
    if config:
        cmd_base += ['--config', config]
    if instance:
        cmd_base += ['--instance', instance]

    while True:
        cmd = cmd_base + ["watch-builds"]

        logger.info("Running %s", cmd)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, ''):
            try:
                json_obj = json.loads(line)
                changeset = json_obj['changetype']
                status = json_obj['status']
                build_name = json_obj['name']
            except Exception as e:
                logger.warn("Error while parsing json '%s': %r", line, e)
                continue

            logger.info("Found build %s in %s", build_name, status)
            build = Build(build_name, cmd_base)
            if status == 'New':
                now = datetime.datetime.now()
                builds_in_new.setdefault(build_name, now)

            elif status != 'New':
                try:
                    del builds_in_new[build_name]

                    # We should reset zabbix item only when the last build in New
                    # has changed its state
                    if not builds_in_new:
                        _send_zabbix_message(zabbix_host, osbs_master, "new_duration", 0)
                except KeyError:
                    pass

            elif status == 'Pending':
                pending.add(build_name)

            elif status == 'Running' and changeset in ['added', 'modified']:
                if build_name in pending:
                    pending_duration = int((build.started_time - build.created_time).total_seconds())
                    _send_zabbix_message(zabbix_host, osbs_master, "pending", pending_duration)
                    logger.info("Pending duration: %s", pending_duration)
                    running_builds.add(build_name)
                pending.discard(build_name)

            elif (status == 'Running' and changeset == 'deleted')\
              or (status in ['Complete', 'Failed', 'Cancelled']):
                pending.discard(build_name)
                running_builds.discard(build_name)

            else:
                logging.warn("Unhandled status: %r", status)

            spent_in_new = [(now - start).total_seconds() for start in builds_in_new.values()]
            if spent_in_new:
                max_spent_in_new = max(spent_in_new)
                logger.info("%s build(s) are in New for, longest: %s sec",
                            len(builds_in_new), max_spent_in_new)
                _send_zabbix_message(zabbix_host, osbs_master,
                                     "new_duration", max_spent_in_new)

            build.send_zabbix_notification(zabbix_host, osbs_master, len(running_builds))

            if build.state == 'Complete':
                try:
                    completed_builds[build_name] = build.completed_time
                    logger.info("Completed time: %s", completed_builds)
                    new_completed_builds = filter_completed_builds(completed_builds)
                    _send_zabbix_message(zabbix_host, osbs_master,
                                         "throughput", len(new_completed_builds))
                    logger.info("Throughput: %s", len(new_completed_builds))
                except Exception as e:
                    logger.warn("Error while removing completed build: %r", e)

            for running_build in running_builds:
                logger.debug("still running: %s", running_build)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument("--instance")
    parser.add_argument("--zabbix-host")
    parser.add_argument("--osbs-master")
    args = parser.parse_args()
    logger.info("Starting osbs-watcher with args %s", args)

    run(args.zabbix_host, args.osbs_master, args.config, args.instance)
