from datetime import datetime

from bs4 import BeautifulSoup

from crawlingnews.sources.SourceParser import SourceParser


class AprParser(SourceParser):
    def get_title(self, soup: BeautifulSoup) -> None | str:
        title_tags = soup.select('h1.ArtP-headline')
        if len(title_tags) == 0:
            return None

        title_tag = title_tags[0]
        return title_tag.text.strip('"').strip()

    def get_authors(self, soup: BeautifulSoup) -> list[str]:
        author_tags = soup.select('.ArtP-authorBy .Link')

        authors = []
        for author_tag in author_tags:
            authors.append(self.process_author(author_tag.text))

        return authors

    def get_date(self, soup: BeautifulSoup) -> None | str:
        date_tags = soup.select('.ArtP-timestamp meta')
        if len(date_tags) == 0:
            return None

        date_tag = date_tags[0]
        date = self.get_attr(date_tag, 'content')
        if date is None:
            return None

        # 将日期处理成YYYYMMDD的格式
        return date.replace('-', '')[:8]

    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        paragraph_tags = soup.select('.ArtP-articleBody > p')
        paragraphs = []

        for p in paragraph_tags:
            if not self.only_a_in_p(p):
                continue
            paragraph = self.process_paragraph(p.text)
            paragraphs.append(paragraph)

        return paragraphs
