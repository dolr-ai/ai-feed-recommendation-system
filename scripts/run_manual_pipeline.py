import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.dependencies import build_runtime_objects
from src.core.settings import get_settings
from src.services.logger_service import LoggerService
from src.utils.kvrocks import build_kvrocks_client


async def main() -> None:
    get_settings.cache_clear()
    settings = get_settings()
    LoggerService().configure(settings.log_level)
    kvrocks = await build_kvrocks_client(settings)
    runtime = build_runtime_objects(kvrocks, settings)
    started = time.time()
    try:
        await runtime["pipeline_service"].run()
        print({"pipeline_status": "success", "elapsed_sec": round(time.time() - started, 2)})
    finally:
        await runtime["chat_api_client"].close()
        if hasattr(kvrocks, "aclose"):
            await kvrocks.aclose()
        else:
            await kvrocks.close()


if __name__ == "__main__":
    asyncio.run(main())
