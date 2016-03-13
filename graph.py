from collections import defaultdict
import json
from osbs.utils import strip_registry_from_image
import sys


class BuildTree(object):
    def __init__(self, builds):
        self.deps = defaultdict(set)
        for build in builds:
            self.add(build)

    def add(self, build):
        try:
            annotations = build['metadata']['annotations']
            base_image_name = annotations['base-image-name']
            repositories = json.loads(annotations['repositories'])
        except KeyError:
            return

        repos = [strip_registry_from_image(repo)
                 for repos in repositories.values()
                 for repo in repos]
        self.deps[strip_registry_from_image(base_image_name)].update(repos)

    def __repr__(self):
        return repr(self.deps)


def run(inputfile):
    with open(inputfile) as fp:
        builds = json.load(fp)

    from pprint import pprint
    pprint(BuildTree(builds).deps)


if __name__ == '__main__':
    run(sys.argv[1])
