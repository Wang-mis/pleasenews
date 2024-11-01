from datetime import datetime

from bs4 import BeautifulSoup

from crawlingnews.sources.SourceParser import SourceParser


class ApnewsParser(SourceParser):
    def get_title(self, soup: BeautifulSoup) -> None | str:
        title_tags = soup.select('h1.Page-headline')
        if len(title_tags) == 0:
            return None

        title_tag = title_tags[0]
        return title_tag.text.strip('"').strip()

    def get_authors(self, soup: BeautifulSoup) -> list[str]:
        author_tags = soup.select('.Page-authors .Link')

        authors = []
        for author_tag in author_tags:
            authors.append(self.process_author(author_tag.text))

        return authors

    def get_date(self, soup: BeautifulSoup) -> None | str:
        date_tags = soup.select('div.Page-dateModified bsp-timestamp')
        if len(date_tags) == 0:
            return None

        date_tag = date_tags[0]
        ts = self.get_attr(date_tag, 'data-timestamp')
        if ts is None:
            return None

        # 将时间戳处理成YYYYMMDD的格式
        try:
            date_obj = datetime.fromtimestamp(int(ts) / 1000)
            return date_obj.strftime('%Y%m%d')
        except ValueError:  # 如果日期解析失败，返回None
            return None

    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        paragraph_tags = soup.select('.RichTextStoryBody > p')
        paragraphs = []

        for p in paragraph_tags:
            paragraph = self.process_paragraph(p.text)
            paragraphs.append(paragraph)

        return paragraphs
