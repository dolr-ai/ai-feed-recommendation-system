import aiohttp


class HttpClientFactory:
    @staticmethod
    def create(timeout_sec: int) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        return aiohttp.ClientSession(timeout=timeout)
