import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.settings import get_settings
from src.utils.kvrocks import (
    build_kvrocks_client,
    feed_meta_key,
    metadata_key,
    ranked_feed_key,
    serve_counts_key,
    stats_snapshot_key,
)


async def main() -> None:
    get_settings.cache_clear()
    settings = get_settings()
    client = await build_kvrocks_client(settings)
    try:
        deleted = await client.delete(
            ranked_feed_key(),
            metadata_key(),
            feed_meta_key(),
            stats_snapshot_key(),
            serve_counts_key(),
        )
        print({"deleted_keys_count": int(deleted)})
    finally:
        if hasattr(client, "aclose"):
            await client.aclose()
        else:
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
