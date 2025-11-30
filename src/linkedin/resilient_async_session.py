from curl_cffi import AsyncSession, Response, requests
from curl_cffi.requests.session import HttpMethod, RequestParams
from typing import Optional, Sequence, Any
from typing_extensions import override, Unpack
from itertools import cycle
import asyncio


class ResilientAsyncSession(AsyncSession[Response]):
    """
    A wrapper around `curl_cffi.AsyncSession` that retries requests that fail due to transient errors.
    
    It rotates through a list of proxies, if provided, and implements exponential backoff for retries.
    """

    RETRY_COUNT = 3

    def __init__(self, proxies: Optional[Sequence[str]] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._proxy_cycle = cycle(proxies) if proxies else cycle([""])

    def _should_retry_request(self, err: Exception, retry: int) -> bool:
        """
        Check whether a request should be retried based on the error and the retry count.
        """

        if retry >= self.RETRY_COUNT:
            return False

        if not isinstance(err, requests.errors.RequestsError):
            return False

        # If there is no response, we assume it's a network error and retry
        if err.response is None:
            return True

        assert isinstance(err.response.status_code, int)
        return err.response.status_code == 429 or err.response.status_code >= 500

    @override
    async def request(self, method: HttpMethod, url: str, *args: Any, **kwargs: Unpack[RequestParams]) -> Any:
        """
        Refer to `curl_cffi.AsyncSession.request` for parameter documentation.

        Ignores any proxy passed during the request if a proxy list was provided during initialization.
        """
        
        for retry in range(self.RETRY_COUNT + 1):
            proxy = next(self._proxy_cycle)
            if proxy:
                kwargs["proxy"] = proxy

            try:
                response = await super().request(
                    method=method,
                    url=url,
                    **kwargs,
                )
                response.raise_for_status()
            except requests.errors.RequestsError as err:
                if self._should_retry_request(err, retry):
                    await asyncio.sleep(2**retry)
                    continue
                raise

            return response
