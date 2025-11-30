from dataclasses import dataclass
import datetime
from enum import Enum
import re
from typing import Optional, Sequence


class EmploymentType(Enum):
    FULL_TIME = "F"
    PART_TIME = "P"
    CONTRACT = "C"
    TEMPORARY = "T"
    INTERNSHIP = "I"
    OTHER = "O"


class ExperienceLevel(Enum):
    INTERNSHIP = 1
    ENTRY_LEVEL = 2
    ASSOCIATE = 3
    MID_SENIOR_LEVEL = 4
    DIRECTOR = 5
    EXECUTIVE = 6


class WorkMode(Enum):
    ON_SITE = 1
    REMOTE = 2
    HYBRID = 3


@dataclass(frozen=True)
class JobFilter:
    """
    Criteria to filter job postings.

    Attributes:
        title: Job title or keywords to search for.
        location: Location to search for jobs in. Defaults to "United States".
        employment_types: Employment types to filter by.
        experience_levels: Experience levels to filter by.
        work_modes: Work modes to filter by.
        few_applicants: Whether to filter for jobs with few applicants (less than 10).
        age: Maximum age of the job postings.
    """

    title: Optional[str] = None
    location: str = "United States"
    employment_types: Optional[Sequence[EmploymentType]] = None
    experience_levels: Optional[Sequence[ExperienceLevel]] = None
    work_modes: Optional[Sequence[WorkMode]] = None
    few_applicants: Optional[bool] = None
    age: Optional[datetime.timedelta] = None

    @staticmethod
    def _join_enum_values(sequence: Sequence[Enum]) -> str:
        """
        Join the values of a sequence of enums into a comma-separated string.
        """

        return ",".join(str(i.value) for i in sequence)

    def to_linkedin_params(self) -> dict[str, str]:
        """
        Convert the job filter to LinkedIn API request parameters.
        """

        params: dict[str, str] = {}

        if self.title is not None:
            params["keywords"] = self.title

        params["location"] = self.location

        if self.employment_types is not None:
            params["f_JT"] = JobFilter._join_enum_values(self.employment_types)

        if self.experience_levels is not None:
            params["f_E"] = JobFilter._join_enum_values(self.experience_levels)

        if self.work_modes is not None:
            params["f_WT"] = JobFilter._join_enum_values(self.work_modes)

        if self.few_applicants is True:
            params["f_JIYN"] = "true"

        if self.age is not None:
            params["f_TPR"] = f"r{int(self.age.total_seconds())}"

        return params


@dataclass(frozen=True)
class Job:
    """
    Basic information about a job posting.

    Attributes:
        url: URL of the job posting.
        title: Title of the job.
        location: Location of the job.
        company_title: Name of the company posting the job.
        company_url: URL of the company's LinkedIn page.
        posted_at: Date the job was posted.
    """

    url: str
    title: str
    location: str
    company_title: str
    company_url: str
    posted_at: datetime.date

    @property
    def id(self) -> str:
        """Unique LinkedIn job ID parsed from the job URL."""

        if match := re.search(r"-?(\d+)(?:[/?]|$)", self.url):
            return match.group(1)
        raise ValueError(f"Could not parse job ID from URL: {self.url}")


@dataclass(frozen=True)
class JobDetails:
    """
    Detailed information about a job posting.

    Attributes:
        description: Full job description in HTML format.
        employment_type: Type of employment.
        experience_level: Experience level required, if specified.
        applicant_count: Number of applicants (25 may mean <= 25, 200 may mean >= 200).
    """

    description: str
    employment_type: EmploymentType
    experience_level: Optional[ExperienceLevel]
    applicant_count: int
