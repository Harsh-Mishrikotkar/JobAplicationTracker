from abc import ABC, abstractmethod


class BaseFetcher(ABC):
    """
    Abstract base class for all job source fetchers.
    Each fetcher is responsible for one thing only:
    hit an endpoint and return raw data exactly as received.
    No normalizing, no filtering — just raw.
    """

    @abstractmethod
    def fetch(self) -> list[dict]:
        """
        Fetch raw job postings from the source.
        Returns a list of raw dicts, one per job posting.
        """
        ...