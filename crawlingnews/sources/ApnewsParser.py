from bs4 import BeautifulSoup

from crawlingnews.sources.SourceParser import SourceParser


class ApnewsParser(SourceParser):
    def get_title(self, soup: BeautifulSoup) -> None | str:
        pass

    def get_author(self, soup: BeautifulSoup) -> None | str:
        pass

    def get_date(self, soup: BeautifulSoup) -> None | str:
        pass

    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        pass
