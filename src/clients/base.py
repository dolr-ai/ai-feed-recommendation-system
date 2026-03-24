from abc import ABC, abstractmethod


class BaseApiClient(ABC):
    @abstractmethod
    async def close(self) -> None:
        ...
