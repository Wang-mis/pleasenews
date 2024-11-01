from bs4 import BeautifulSoup

from crawlingnews.sources.Source import SourceParser


class Apnews(SourceParser):
    def get_title(self, soup: BeautifulSoup):
        pass

    def get_author(self, soup: BeautifulSoup):
        pass

    def get_date(self, soup: BeautifulSoup):
        pass

    def get_paragraphs(self, soup: BeautifulSoup):
        pass