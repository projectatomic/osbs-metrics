import argparse
import subprocess
import json
import thread
import logging



import os
import re
from time import sleep, time, strptime
from calendar import timegm
from tempfile import NamedTemporaryFile

logger = logging.getLogger('osbs-metrics')
logger.handlers = []
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class BuildLog(object):
    def __init__(self, logfile):
        self.logfile = logfile
        self.data = None
        self._trawl()

    def _trawl(self):
        if self.data is not None:
            return

        cache = self.logfile + '.cache'
        try:
            with open(cache) as cf:
                self.data = json.load(cf)
                return
        except (IOError, ValueError):
            pass

        self.data = {
            'upload_size_mb': 'nan',
        }
        name_re = re.compile(r'selflink.: u./oapi/v1/namespaces/default/builds/([^,]*).,')
        size_re = re.compile(r' - dockpulp - INFO - uploading a (.*)M image')
        plugin_re = re.compile(r'([0-9 :-]*),[0-9]+ - atomic_reactor.plugin - DEBUG - running plugin \'(.*)\'')
        error_re = re.compile(r'ERROR - .*plugin \'(.*)\' raised an exception: ([^(]*)')
        image_re = re.compile(r'pulp_push - INFO - image names: \[.*\'([^\']*):latest')
        buildfail_re = re.compile(r'INFO - build was unsuccess?ful')
        with open(self.logfile) as lf:
            log = lf.read()
            name = name_re.search(log)
            if name:
                self.data['name'] = name.groups()[0]

            size = size_re.search(log)
            if size:
                self.data['upload_size_mb'] = size.groups()[0]

            image = image_re.search(log)
            if image:
                self.data['image'] = image.groups()[0]

            last_plugin = None
            plugins = plugin_re.findall(log)
            for timestamp, plugin_name in plugins:
                t = timegm(strptime(timestamp, "%Y-%m-%d %H:%M:%S"))
                if last_plugin is not None:
                    self.data[last_plugin[1]] = t - last_plugin[0]

                last_plugin = (t, plugin_name)

            error = error_re.search(log)
            if error:
                (self.data['failed_plugin'],
                 self.data['exception']) = error.groups()
            else:
                buildfail = buildfail_re.search(log)
                if buildfail:
                    self.data['failed_plugin'] = 'dockerbuild'
                    self.data['exception'] = ''

        os.remove(self.logfile)


class Build(object):
    def __init__(self, build_name, cmd_base, data=None):
        logger.info("Creating build %s:%s" % (build_name, data))
        logger.info("cmd base '%s'" % cmd_base)
        self.cmd_base = cmd_base
        if not data:
            self.name = build_name
            self._data = {}
            self.load_build_data()
        else:
            self._data = data
            self.name = self._data['metadata']['name']

    def load_build_data(self):
        logger.info("Loading data for build %s", self.name)
        cmd = self.cmd_base + ["get-build", self.name]
        try:
            stdout = subprocess.check_output(cmd)
            self._data = json.loads(stdout)
        except subprocess.CalledProcessError as e:
            logger.warn("Error while fetching build data: %s", repr(e))
            logger.warn('Exit code: %s' % e.returncode)
            logger.warn('Output: %s' % e.output)

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
        logfile = "{name}.log".format(name=self.name)
        if not os.access(logfile, os.R_OK):
            cmd = self.cmd_base + ['build-logs', self.name]
            with open(logfile, 'w') as fp:
                print(' '.join(cmd))
                p = subprocess.Popen(cmd, stdout=fp)
                p.communicate()

        return BuildLog(logfile).data

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
                zabbix_result['pulp_push_speed'] =\
                    self.upload_size_mb / float(zabbix_result['pulp_push'])
            except Exception as e:
                logger.warn('Error calculating push speed: %s' % repr(e))
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
            logger.info("Sending build data: %s" % cmd)
            try:
                output = subprocess.check_output(cmd, shell=True)
                logger.info('Output:\n%s' % output)
            except subprocess.CalledProcessError as e:
                logger.warn('Error while sending build data: %s' % repr(e))
                logger.warn('Exit code: %s' % e.returncode)
                logger.warn('Output: %s' % e.output)

        sleep(10)
        # Now we need to send zeros so the data from previous run won't pollute next runs
        logger.info("Sending zero data")
        with NamedTemporaryFile(delete=True) as temp_zabbix_data:
            for k, v in zabbix_result.iteritems():
                if k not in ['concurrent', 'pulp_push_speed']:
                    temp_zabbix_data.write("- %s 0\n" % k)
            temp_zabbix_data.flush()

            cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -i "%s"' % (
                  zabbix_host, osbs_master, temp_zabbix_data.name)
            try:
                output = subprocess.check_output(cmd, shell=True)
                logger.info('Output:\n%s' % output)
            except subprocess.CalledProcessError as e:
                logger.warn('Error while sending build data: %s' % repr(e))
                logger.warn('Exit code: %s' % e.returncode)
                logger.warn('Output: %s' % e.output)


def _send_zabbix_message(zabbix_host, osbs_master, key, value, print_command=True):
    cmd = 'zabbix_sender -z %s -p 10051 -s "%s" -k %s -o "%s"' % (
          zabbix_host, osbs_master, key, value)
    if print_command:
        logger.info("running %s:" % cmd)
    try:
        output = subprocess.check_output(cmd, shell=True)
        if print_command:
            logger.info('Output:\n%s' % output)
    except subprocess.CalledProcessError as e:
        logger.warn('Error while sending zabbix message: %s' % repr(e))
        logger.warn('Exit code: %s' % e.returncode)
        logger.warn('Output: %s' % e.output)


def filter_completed_builds(completed_builds):
    # Remove all completed_builds which are not within this hour
    now = int(time())
    return {k: v for k, v in completed_builds.items() if (v - now) < 3600}


def heartbeat(zabbix_host, osbs_master):
    while True:
        _send_zabbix_message(zabbix_host, osbs_master,
                             "heartbeat", int(time()), print_command=False)
        sleep(10)


def run(zabbix_host, osbs_master, config, instance):
    running_builds = set()
    pending = {}
    completed_builds = {}

    thread.start_new_thread(heartbeat, (zabbix_host, osbs_master, ))

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
                logger.warn("Error while parsing json '%s': %s", line, repr(e))
                continue
            if status == 'Pending':
                if build_name not in pending.keys():
                    pending[build_name] = int(time())
            elif status == 'Running' and changeset == 'modified':
                running_builds.add(build_name)
                if build_name in pending.keys():
                    pending_duration = int(time()) - pending[build_name]
                    _send_zabbix_message(zabbix_host, osbs_master, "pending", pending_duration)
                    del pending[build_name]
            elif status == 'Running' and changeset == 'deleted':
                try:
                    running_builds.remove(build_name)
                    completed_builds[build_name] = int(time())
                    completed_builds = filter_completed_builds(completed_builds)
                    _send_zabbix_message(zabbix_host, osbs_master,
                                         "throughput", len(completed_builds))
                except Exception as e:
                    logger.warn("Error while removing completed build: %s", repr(e))

            build = Build(build_name, cmd_base)
            build.send_zabbix_notification(zabbix_host, osbs_master, len(running_builds))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument("--instance")
    parser.add_argument("--zabbix-host")
    parser.add_argument("--osbs-master")
    args = parser.parse_args()
    logger.info("Starging osbs-watcher with args %s", args)

    run(args.zabbix_host, args.osbs_master, args.config, args.instance)
