from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import stat
import subprocess
import tempfile
from pathlib import Path

from redis.asyncio.cluster import RedisCluster


REMOTE_INSPECT_COMMAND = (
    "printf '6868\\n' | sudo -S docker inspect offchain-agent-offchain-agent-1"
)


def fetch_offchain_container_env() -> dict[str, str]:
    inspect_json = subprocess.check_output(
        [
            "ssh",
            "off-chain-1",
            f"bash -lc {shlex.quote(REMOTE_INSPECT_COMMAND)}",
        ],
        text=True,
    )
    container = json.loads(inspect_json)[0]

    env: dict[str, str] = {}
    for item in container["Config"]["Env"]:
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        env[key] = value
    return env


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("video_ids", nargs="+")
    args = parser.parse_args()

    env = fetch_offchain_container_env()
    hosts = [host.strip() for host in env["KVROCKS_HOSTS"].split(",") if host.strip()]

    with tempfile.TemporaryDirectory(prefix="offchain-kvrocks-") as temp_dir:
        temp_path = Path(temp_dir)
        ca_path = temp_path / "ca.crt"
        cert_path = temp_path / "client.crt"
        key_path = temp_path / "client.key"

        ca_path.write_text(env["KVROCKS_CA_CERT"])
        cert_path.write_text(env["KVROCKS_CLIENT_CERT"])
        key_path.write_text(env["KVROCKS_CLIENT_KEY"])

        for path in (ca_path, cert_path, key_path):
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

        client = RedisCluster(
            host=hosts[0],
            port=6666,
            password=env["KVROCKS_PASSWORD"],
            ssl=True,
            ssl_check_hostname=False,
            ssl_ca_certs=str(ca_path),
            ssl_certfile=str(cert_path),
            ssl_keyfile=str(key_path),
            decode_responses=True,
        )

        try:
            ping = await client.ping()
            results: dict[str, dict[str, str]] = {}
            for video_id in args.video_ids:
                key = f"offchain:metadata:video_details:{video_id}"
                results[video_id] = await client.hgetall(key)

            print(
                json.dumps(
                    {
                        "host_count": len(hosts),
                        "ping": ping,
                        "results": results,
                    },
                    indent=2,
                )
            )
        finally:
            await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
