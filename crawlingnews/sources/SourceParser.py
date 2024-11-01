import re
from abc import abstractmethod

from bs4 import BeautifulSoup, Tag


class SourceParser:
    """ 媒体新闻解析器的基类 """

    def __init__(self):
        super().__init__()

    @staticmethod
    def only_a_in_p(p: Tag) -> bool:
        other_tags = p.find_all(recursive=False)
        other_tags = [tag for tag in other_tags if tag.name != 'a']
        return len(other_tags) == 0

    @staticmethod
    def get_attr(tag: Tag, attr_name: str) -> None | str:
        attr = tag.get(attr_name)
        if attr is None:
            return None

        if type(attr) == list:
            if len(attr) == 0:
                return None
            return attr[0]

        return attr

    @staticmethod
    def process_author(a: str, only_ascii: bool = False) -> str:
        if only_ascii:
            a = re.sub(r'[^\x00-\x7F]+', ' ', a)
        a = re.sub(r'by', '', a, flags=re.IGNORECASE).strip()
        return ' '.join(au.capitalize() for au in a.split())

    @staticmethod
    def process_paragraph(p: str, only_ascii: bool = False) -> str:
        p = p.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
        if only_ascii:
            p = re.sub(r'[^\x00-\x7F]+', ' ', p)
        return re.sub(r'\s+', ' ', p).strip()

    @abstractmethod
    def get_title(self, soup: BeautifulSoup) -> None | str:
        pass

    @abstractmethod
    def get_authors(self, soup: BeautifulSoup) -> list[str]:
        pass

    @abstractmethod
    def get_date(self, soup: BeautifulSoup) -> None | str:
        pass

    @abstractmethod
    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        pass
