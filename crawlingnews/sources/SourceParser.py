import re
from abc import abstractmethod

from bs4 import BeautifulSoup


class SourceParser:
    def __init__(self):
        super().__init__()

    @staticmethod
    def process_paragraph(p: str) -> str:
        p = p.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
        p = re.sub(r'[^\x00-\x7F]+', ' ', p)
        return re.sub(r'\s+', ' ', p).strip()

    @abstractmethod
    def get_title(self, soup: BeautifulSoup) -> None | str:
        pass

    @abstractmethod
    def get_author(self, soup: BeautifulSoup) -> None | str:
        pass

    @abstractmethod
    def get_date(self, soup: BeautifulSoup) -> None | str:
        pass

    @abstractmethod
    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        pass
