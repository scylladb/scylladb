from pathlib import Path

import yaml


def get_docker_version(dependency: str, key="image"):
    # values prefix in filename is important for dependbot to find this file
    # see:
    # https://github.com/dependabot/dependabot-core/blob/9ddaf03513236ea2190bd23901a7e753022bd1fa/docker/lib/dependabot/docker/utils/helpers.rb#L9C30-L9C31
    with Path(__file__).parent.joinpath("values_docker_versions.yaml").open("r") as f:
        return yaml.safe_load(f)[dependency][key]
