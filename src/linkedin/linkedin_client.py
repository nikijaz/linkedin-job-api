from typing import Any, Optional

from linkedin.parsing import parse_jobs, parse_job_details
from linkedin.models import Job, JobFilter, JobDetails
from typing import Sequence
from linkedin.resilient_async_session import ResilientAsyncSession
import asyncio

import math

LINKEDIN_PAGE_SIZE = 10
LINKEDIN_JOB_QUERY_LIMIT = 1000


class LinkedInClient:
    """
    Asynchronous client for fetching job postings and details from LinkedIn.

    Handles pagination, rate limiting, and concurrent requests under the hood.
    """

    _session: ResilientAsyncSession
    _semaphore: asyncio.Semaphore

    def __init__(self, timeout: float = 30, proxies: Optional[Sequence[str]] = None, max_concurrent_requests: int = 10):
        """
        Args:
            timeout: Timeout for HTTP requests in seconds.
            proxies: List of proxy URLs to use for requests.
            max_concurrent_requests: Client-wide limit for concurrent requests.
        """

        self._session = ResilientAsyncSession(timeout=timeout, proxies=proxies)
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def close(self) -> None:
        """
        Close the client and release any resources.
        """

        await self._session.close()

    async def __aenter__(self) -> "LinkedInClient":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def _fetch_jobs_page(self, filter: JobFilter, offset: int = 0) -> list[Job]:
        """
        Fetch a single page of job postings from LinkedIn based on the provided filter and offset.
        """

        async with self._semaphore:
            response = await self._session.get(
                url="https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
                params={
                    **filter.to_linkedin_params(),
                    "start": offset,
                },
            )
        return parse_jobs(response.text)

    async def _fetch_job_details(self, id: str) -> JobDetails:
        """
        Fetch detailed information for a specific job posting by its ID.
        """

        async with self._semaphore:
            response = await self._session.get(
                url=f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{id}",
            )
        return parse_job_details(response.text)

    async def fetch_jobs(
        self, filter: JobFilter, offset: int = 0, limit: Optional[int] = LINKEDIN_PAGE_SIZE
    ) -> list[Job]:
        """
        Fetch job postings from LinkedIn.

        Args:
            filter: Filter criteria for job postings.
            offset: Starting index for fetching job postings.
            limit: Maximum number of job postings to fetch. If None, fetches up to the LinkedIn job query limit.

        Returns:
            A list of Job objects matching the filter criteria.

        Raises:
            ValueError: If offset or limit are negative, or if the total exceeds LinkedIn's job query limit.
        """

        if limit is None:
            limit = LINKEDIN_JOB_QUERY_LIMIT - offset

        if offset < 0 or limit < 0:
            raise ValueError("Offset and limit must be non-negative")

        if offset + limit > LINKEDIN_JOB_QUERY_LIMIT:
            raise ValueError("LinkedIn only allows fetching up to 1000 jobs")

        total_pages = math.ceil(limit / LINKEDIN_PAGE_SIZE)

        tasks = [
            asyncio.create_task(self._fetch_jobs_page(filter, offset + i * LINKEDIN_PAGE_SIZE))
            for i in range(total_pages)
        ]
        results = await asyncio.gather(*tasks)

        jobs = [job for result in results for job in result]

        return jobs[:limit]

    async def fetch_job_details(self, id: str) -> JobDetails:
        """
        Fetch detailed information for a specific job posting by its ID.
        """

        return await self._fetch_job_details(id)

    async def fetch_jobs_details(self, ids: Sequence[str]) -> list[JobDetails]:
        """
        Fetch detailed information for multiple job postings by their IDs.
        """

        tasks = [asyncio.create_task(self._fetch_job_details(id)) for id in ids]
        return await asyncio.gather(*tasks)
