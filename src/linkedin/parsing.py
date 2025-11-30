import re
from bs4 import BeautifulSoup, Tag
import datetime
from linkedin.models import ExperienceLevel, Job, JobDetails, EmploymentType
from typing import Optional

STR_TO_EXPERIENCE_LEVEL_MAP = {
    "Internship": ExperienceLevel.INTERNSHIP,
    "Entry level": ExperienceLevel.ENTRY_LEVEL,
    "Associate": ExperienceLevel.ASSOCIATE,
    "Mid-Senior level": ExperienceLevel.MID_SENIOR_LEVEL,
    "Director": ExperienceLevel.DIRECTOR,
    "Executive": ExperienceLevel.EXECUTIVE,
}

STR_TO_EMPLOYMENT_TYPE_MAP = {
    "Full-time": EmploymentType.FULL_TIME,
    "Part-time": EmploymentType.PART_TIME,
    "Contract": EmploymentType.CONTRACT,
    "Temporary": EmploymentType.TEMPORARY,
    "Internship": EmploymentType.INTERNSHIP,
    "Other": EmploymentType.OTHER,
}


def parse_jobs(raw: str) -> list[Job]:
    """
    Parse job listings from raw HTML.
    """

    html = BeautifulSoup(raw, "lxml")

    jobs: list[Job] = []
    for element in html.find_all("li"):
        if not isinstance(element, Tag):
            raise ValueError(f"Could not parse job: {element}")
        jobs.append(_parse_job(element))
    return jobs


def _parse_job(html: Tag) -> Job:
    """
    Parse a single job listing from a BeautifulSoup Tag.
    """

    url: Optional[str] = None
    title: Optional[str] = None
    location: Optional[str] = None
    company_title: Optional[str] = None
    company_url: Optional[str] = None
    posted_at: Optional[datetime.date] = None

    # URL
    element = html.find("a", class_=re.compile("_full-link"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find job URL")
    if isinstance(href := element.get("href"), str):
        url = href
    if url is None:
        raise ValueError("Could not parse job URL")

    # Title
    element = html.find("h3", class_=re.compile("_title"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find job title")
    title = element.getText(strip=True)
    if not title:
        raise ValueError("Could not parse job title")

    # Location
    element = html.find("span", class_=re.compile("_location"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find job location")
    location = element.getText(strip=True)
    if not location:
        raise ValueError("Could not parse job location")

    # Company title
    element = html.find("h4", class_=re.compile("_subtitle"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find company title")
    element = element.find("a")
    if not isinstance(element, Tag):
        raise ValueError("Could not find company title")
    company_title = element.getText(strip=True)
    if not company_title:
        raise ValueError("Could not parse company title")

    # Company URL
    if isinstance(element, Tag) and isinstance(href := element.get("href"), str):
        company_url = href
    if company_url is None:
        raise ValueError("Could not parse company URL")

    # Posted at
    element = html.find("time", class_=re.compile("listdate"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find job age")
    if isinstance(dt := element.get("datetime"), str):
        posted_at = datetime.datetime.strptime(dt, "%Y-%m-%d").date()
    if posted_at is None:
        raise ValueError("Could not parse job age")

    return Job(
        url=url,
        title=title,
        location=location,
        company_title=company_title,
        company_url=company_url,
        posted_at=posted_at,
    )


def parse_job_details(raw: str) -> JobDetails:
    """
    Parse job details from raw HTML.
    """

    html = BeautifulSoup(raw, "lxml")

    description: Optional[str] = None
    employment_type: Optional[EmploymentType] = None
    experience_level: Optional[ExperienceLevel] = None
    applicant_count: Optional[int] = None

    # Description
    element = html.find("div", class_=re.compile("markup"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find job description")
    description = element.decode_contents().strip()
    if not description:
        raise ValueError("Could not parse job description")

    # Employment type and experience level
    criterias = html.find_all("li", class_=re.compile("job-criteria-item"))
    for item in criterias:
        if not isinstance(item, Tag):
            raise ValueError("Could not find employment type or experience level")

        key_element = item.find("h3", class_=re.compile("job-criteria-subheader"))
        value_element = item.find("span", class_=re.compile("job-criteria-text"))
        if not key_element or not value_element:
            raise ValueError("Could not parse employment type or experience level")

        key = key_element.getText(strip=True)
        value = value_element.getText(strip=True)

        if key == "Employment type":
            employment_type = STR_TO_EMPLOYMENT_TYPE_MAP.get(value)
        if key == "Seniority level":
            experience_level = STR_TO_EXPERIENCE_LEVEL_MAP.get(value)
    if employment_type is None:
        raise ValueError("Could not parse employment type")

    # Applicant count
    element = html.find(class_=re.compile("num-applicants"))
    if not isinstance(element, Tag):
        raise ValueError("Could not find applicant count")
    match = re.search(r"(\d+)", element.getText())
    if match is None:
        raise ValueError("Could not parse applicant count")
    applicant_count = int(match.group(1))

    return JobDetails(
        description=description,
        employment_type=employment_type,
        experience_level=experience_level,
        applicant_count=applicant_count,
    )
