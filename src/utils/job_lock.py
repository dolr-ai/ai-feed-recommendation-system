from src.utils.kvrocks import job_lock_key


async def acquire_job_lock(kvrocks_client, job_name: str, ttl: int) -> bool:
    return bool(await kvrocks_client.set(job_lock_key(job_name), "1", ex=ttl, nx=True))


async def release_job_lock(kvrocks_client, job_name: str) -> None:
    await kvrocks_client.delete(job_lock_key(job_name))
