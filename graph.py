from collections import defaultdict
import json
from osbs.utils import strip_registry_from_image
import sys
import datetime
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import re


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


class BuildTree(object):
    def __init__(self, builds, pulp_base_url):
        self.deps = defaultdict(set)
        self.seen = set()
        self.when = {}
        self.known_pulp_layers = {}
        self.found_image_name_sizes = {}
        self.pulp_upload_size = {}
        self.duration = {}
        self.upload_size = {}
        self.layer_size = {}
        self.excess_repos = []
        self.pulp_base_url = pulp_base_url
        # A dict to store the reference to the actual upload_size for each uploaded tag
        self.tags_aliases = {}
        builds = [build for build in builds
                  if ('status' in build and
                      'startTimestamp' in build['status'])]
        builds.sort(key=lambda x: x['status']['startTimestamp'],
                    reverse=True)
        for build in builds:
            self.add(build)

    def _get_layer_info(self, layer_id, pulp_repo_url):
        layer_json = {}
        try:
            size = None
            image_name = None
            parent_id = None
            if layer_id in self.known_pulp_layers.keys():
                image_name = self.known_pulp_layers[layer_id]
                size = self.found_image_name_sizes[image_name]
                return (None, size, image_name)
            else:
                pulp_url = pulp_repo_url + '/%s/json' % layer_id
                r = requests.get(pulp_url, verify=False)
                r.raise_for_status()
                layer_json = r.json()
        except Exception as e:
            sys.stderr.write("  _get_layer_info(%s): %s" % (layer_id, repr(e)))
            return (None, None, None)

        try:
            size = layer_json.get('Size', 0)
            parent_id = layer_json.get('parent', None)
            labels = layer_json['config'].get('Labels', {'Name': '', 'Version': '', 'Release': ''})
            image_name = '%s:%s-%s' % (labels['Name'], labels['Version'], labels['Release'])
        except Exception as e:
            sys.stderr.write("  _get_layer_info(%s): %s" % (layer_id, repr(e)))

        return (parent_id, size, image_name)

    def _get_size_with_parent_layers(self, image_name, parent_layer):
        parent_layer_size = 0
        while parent_layer:
            parent_image_name = self.known_pulp_layers.get(parent_layer, None)
            if not parent_image_name:
                # Lookup parent layer image in current repo
                parent_image_name = '-'.join(image_name.split(':')[0].split('/'))
            pulp_repo_url = '%s/pulp/docker/v1/redhat-%s' % (
                self.pulp_base_url, parent_image_name)
            (parent_layer, current_parent_layer_size, _) = self._get_layer_info(
                parent_layer, pulp_repo_url)
            parent_layer_size += current_parent_layer_size
        return parent_layer_size

    def _get_upload_size(self, build):
        annotations = build['metadata']['annotations']
        image_id = annotations['image-id']
        if image_id in self.pulp_upload_size.keys():
            return (0, self.pulp_upload_size[image_id])

        repos_json = json.loads(annotations['repositories'])
        unique_repos = repos_json['unique']
        if not unique_repos:
            self.pulp_upload_size[image_id] = 0
            return (0, 0)

        repo = repos_json['primary'][0]
        expected_image_name = "%s/%s:%s-%s" % (
            repo.split('/')[-2],
            repo.split('/')[-1].split(':')[0],
            repo.split('/')[-1].split(':')[1].split('-')[0],
            '-'.join(repo.split('/')[-1].split(':')[1].split('-')[1:])
        )

        full_image_name = unique_repos[0]
        image_name = '-'.join(strip_registry_from_image(full_image_name).split('/'))
        if not self.pulp_base_url:
            # Base url for pulp is not specified - fall back to an old method
            try:
                size = json.loads(annotations['tar_metadata'])['size']
                return (size, size)
            except:
                return (0, 0)

        pulp_repo_url = '%s/pulp/docker/v1/redhat-%s' % (self.pulp_base_url, image_name.split(':')[0])

        if image_id in self.known_pulp_layers.keys():
            size = self.found_image_name_sizes[expected_image_name]
            self.pulp_upload_size[image_id] = size
            sys.stderr.write("  found cached size for %s: %s\n" % (image_id, size))
            (parent_layer, layer_size, image_name) = self._get_layer_info(image_id, pulp_repo_url)
            total_size = self._get_size_with_parent_layers(image_name, parent_layer)
            return (size, total_size)

        pulp_url = '%s/%s/json' % (pulp_repo_url, image_id)
        sys.stderr.write("Looking for image size at %s\n" % pulp_url)
        try:
            r = requests.get(pulp_url, verify=False)
            r.raise_for_status()
            size = r.json()['Size']
            self.pulp_upload_size[image_id] = size
            (parent_layer, layer_size, image_name) = self._get_layer_info(image_id, pulp_repo_url)
            total_size = self._get_size_with_parent_layers(image_name, parent_layer)
            return (total_size, size)
        except requests.HTTPError:
            try:
                sys.stderr.write("   looking for layers at %s with image_name %s\n" % (
                    pulp_repo_url, expected_image_name))
                # Wrong image_id, go to repo's page and check every layer id
                r = requests.get(pulp_repo_url, verify=False)
                matches = re.findall(r'href="(.+)/"', r.text)
                sys.stderr.write("   found %s layers\n" % len(matches))
                # Skip the header link
                for layer_id in matches[1:]:
                    (parent_layer, layer_size, image_name) = self._get_layer_info(
                        layer_id, pulp_repo_url)

                    self.found_image_name_sizes[image_name] = layer_size
                    self.known_pulp_layers[layer_id] = image_name
                    if image_name != expected_image_name:
                        sys.stderr.write("   %s != %s\n" % (image_name, expected_image_name))
                        continue
                    sys.stderr.write("  found layer with size %s\n" % layer_size)
                    parent_layer_size = self._get_size_with_parent_layers(image_name, parent_layer)
                    sys.stderr.write("  parent's layer size: %s\n" % parent_layer_size)
                    total_size = layer_size + parent_layer_size
                    self.pulp_upload_size[image_id] = total_size
                    return (total_size, layer_size)
                else:
                    raise RuntimeError("No matching layer found")
            except Exception as e:
                sys.stderr.write("    cannot find layer info at this url: %s\n" % (repr(e)))
                self.pulp_upload_size[image_id] = 0
                return (0, 0)

    def add(self, build):
        try:
            annotations = build['metadata']['annotations']
            base_image_name = annotations['base-image-name']
            repositories = json.loads(annotations['repositories'])
            when = build['status']['startTimestamp']
            duration = int(build['status']['duration']) / (10**9)
            (upload_size, layer_size) = self._get_upload_size(build)
        except KeyError:
            return

        repos = set([strip_registry_from_image(repo)
                     for repo in repositories['primary']])
        duplicates = self.seen.intersection(repos)
        repos -= duplicates
        self.seen.update(repos)
        self.deps[strip_registry_from_image(base_image_name)].update(repos)

        if list(repos):
            first_tag = list(repos)[0]
            self.upload_size[first_tag] = upload_size
            self.layer_size[first_tag] = layer_size
            self.duration[first_tag] = duration
            for repo in repos:
                self.when[repo] = when
                self.tags_aliases[repo] = first_tag

    def _trim_layers(self, base):
        layers = self.deps[base]
        excess = set()
        for layer in layers:
            name, version = layer.split(':', 1)
            if version == 'latest':
                pass
            elif not self.deps.get(layer):
                # Leaf node
                excess.add(layer)

        self.deps[base] -= excess
        self.excess_repos += excess
        return excess

    def trim_excess_tags(self):
        while True:
            images = [image for image in self.deps.keys()]
            any_trimmed = False
            for base in images:
                if self.deps.get(base):
                    if self._trim_layers(base):
                        any_trimmed = True

            if not any_trimmed:
                break

    def get_trimmed_layer_size(self):
        trimmed_layers = []
        images = [image for image in self.deps.keys()]
        for base in images:
            layers = self.deps.get(base, [])
            for layer in layers:
                if layer in trimmed_layers:
                    continue
                if self.layer_size.get(base) and self.layer_size.get(layer):
                    self.layer_size[layer] -= self.layer_size[base]
                trimmed_layers.append(layer)

        return sum([v for k, v in self.layer_size.items()])

    def calculate_totals(self):
        trimmed_duration = sum([v for k, v in self.duration.items()])
        trimmed_upload_size = sum([v for k, v in self.upload_size.items()])
        trimmed_layer_size = sum([v for k, v in self.layer_size.items()])

        for layer in self.excess_repos:
            if layer in self.duration.keys():
                trimmed_duration -= self.duration[layer]
            if layer in self.upload_size.keys():
                trimmed_upload_size -= self.upload_size[layer]
            if layer in self.layer_size.keys():
                trimmed_layer_size -= self.layer_size[layer]

        return (trimmed_duration, trimmed_upload_size, trimmed_layer_size)

    def __repr__(self):
        return repr(self.deps)

    def get_build_duration(self, tag_name):
        if tag_name in self.duration.keys():
            return str(datetime.timedelta(seconds=self.duration[tag_name]))
        else:
            if tag_name in self.tags_aliases.keys():
                return str(datetime.timedelta(seconds=self.duration[self.tags_aliases[tag_name]]))
            else:
                return ""

    def get_upload_size(self, tag_name):
        if tag_name in self.upload_size.keys():
            return sizeof_fmt(self.upload_size[tag_name])
        else:
            if tag_name in self.tags_aliases.keys():
                return sizeof_fmt(self.upload_size[self.tags_aliases[tag_name]])
            else:
                return ""

    def as_graph_easy_txt(self,
                          include_datestamp=False,
                          include_duration=False,
                          include_upload=False):
        txt = ''

        def formatwhen(name):
            if include_datestamp:
                try:
                    return "\\n{when}".format(when=self.when[name][:10])
                except KeyError:
                    return ""
            else:
                return ""

        def formatduration(name):
            if include_duration:
                return "\\n{duration}".format(duration=self.get_build_duration(name))
            else:
                return ""

        def formatupload(name):
            if include_upload:
                return "\\n{upload}".format(upload=self.get_upload_size(name))
            else:
                return ""

        for base, layers in self.deps.items():
            for layer in layers:
                txt += "[ {base}{when}{duration}{upload} ]".format(base=base,
                                                                   when=formatwhen(base),
                                                                   duration=formatduration(base),
                                                                   upload=formatupload(base))
                txt += " --> "
                txt += "[ {layer}{when}{duration}{upload} ]".format(layer=layer,
                                                                    when=formatwhen(layer),
                                                                    duration=formatduration(layer),
                                                                    upload=formatupload(layer))
                txt += "\n"

        return txt


def run(inputfile=None, pulp_base_url=None):
    if inputfile is not None:
        with open(inputfile) as fp:
            builds = json.load(fp)
    else:
        builds = json.load(sys.stdin)

    tree = BuildTree(builds, pulp_base_url)
    tree.trim_excess_tags()
    print(tree.as_graph_easy_txt(
          include_datestamp=True, include_duration=True, include_upload=True))

    (total_duration, total_upload_size, total_layers_size) = tree.calculate_totals()

    sys.stderr.write("Total duration: %s\n" % str(datetime.timedelta(seconds=total_duration)))
    sys.stderr.write("Total upload size: %s (%s to Pulp and %s to Brew/Koji)\n" % (
        sizeof_fmt(total_upload_size + total_layers_size),
        sizeof_fmt(total_layers_size),
        sizeof_fmt(total_upload_size)))

if __name__ == '__main__':
    args = [sys.argv[1] if len(sys.argv) > 1 else None,
            sys.argv[2] if len(sys.argv) > 2 else None]
    run(*args)
