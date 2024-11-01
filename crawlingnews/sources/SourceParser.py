from abc import abstractmethod

from bs4 import BeautifulSoup


class Source:
    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_title(self, soup: BeautifulSoup):
        pass

    @abstractmethod
    def get_author(self, soup: BeautifulSoup):
        pass

    @abstractmethod
    def get_date(self, soup: BeautifulSoup):
        pass

    @abstractmethod
    def get_paragraphs(self, soup: BeautifulSoup):
        pass
