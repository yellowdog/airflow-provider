"""List and sweep YellowDog resources left behind by system tests."""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from yellowdog_client.model import ApiKey, ServicesSchema
from yellowdog_client.platform_client import PlatformClient

from tests.system.cleanup import (
    find_compute_requirements,
    find_work_requirements,
    find_worker_pools,
    sweep,
)
from tests.system.config import ConfigError, config_path, load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fragment", default="systest-")
    parser.add_argument("--yes", action="store_true", help="skip confirmation")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("system-test-cleanup")

    path = config_path()
    try:
        config = load_config(path)
    except (ConfigError, OSError, ValueError) as e:
        # tomllib.TOMLDecodeError is a ValueError
        print(f"system-test-cleanup: bad config {path}: {e}", file=sys.stderr)
        return 2
    client = PlatformClient.create(
        ServicesSchema(defaultUrl=config.api_url),
        ApiKey(config.key_id, config.key_secret),
    )
    try:
        ns, fragment = config.namespace, args.fragment
        wrs = find_work_requirements(client, ns, fragment)
        wps = find_worker_pools(client, ns, fragment)
        crs = find_compute_requirements(client, ns, fragment)
        for w in wrs:
            print(f"work requirement   {w.id}  {w.name}  {w.status}")
        for p in wps:
            print(f"worker pool        {p.id}  {p.name}  {p.status}")
        for c in crs:
            print(f"compute requirement {c.id}  {c.name}  {c.status}")
        if not (wrs or wps or crs):
            print(f"Nothing live matching '{fragment}' in namespace '{ns}'.")
            return 0
        if not args.yes and input("Clean up these resources? [y/N] ").lower() != "y":
            print("Aborted.")
            return 1
        print(sweep(client, ns, fragment, log))
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
