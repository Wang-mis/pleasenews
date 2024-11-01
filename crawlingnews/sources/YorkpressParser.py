import re
from bs4 import BeautifulSoup
from crawlingnews.sources.SourceParser import SourceParser


class YorkpressParser(SourceParser):
    def get_title(self, soup: BeautifulSoup) -> None | str:
        head_tag = soup.select('h1.mar-article__headline')
        if len(head_tag) == 0:
            return None
        return head_tag[0].text

    def get_author(self, soup: BeautifulSoup) -> None | str:
        author_tags = soup.select('.author-name')
        author_no_job_tags = soup.select('.author-no-job')
        author_no_image_tags = soup.select('.author-no-images')

        author_tag = None
        if len(author_tags) > 0:
            author_tag = author_tags[0]
        elif len(author_no_job_tags) > 0:
            author_tag = author_no_job_tags[0]
        elif len(author_no_image_tags) > 0:
            author_tag = author_no_image_tags[0]

        if author_tag is not None:
            return author_tag.text.replace('By', '').strip()

        return None

    def get_date(self, soup: BeautifulSoup) -> None | str:
        time_tag = soup.select('.mar-article__timestamp time')
        if len(time_tag) == 0:
            return None

        time_tag = time_tag[0]
        datetime = time_tag.get('datetime')
        if type(datetime) == list:
            if len(datetime) == 0:
                return None

            datetime = datetime[0]

        # 将日期处理成YYYYMMDD的格式
        return datetime.split(' ')[0].replace('-', '')

    @staticmethod
    def process_paragraph(p: str) -> str:
        p = p.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
        return re.sub(r'\s+', ' ', p).strip()

    def get_paragraphs(self, soup: BeautifulSoup) -> list[str]:
        paragraphs = []
        first = soup.select('p.article-first-paragraph')
        if len(first) == 1:
            paragraphs.append(self.process_paragraph(first[0].text))

        others = soup.select('#subscription-content p')
        for other in others:
            # 如果p标签内还有其他标签，直接跳过
            if other.find():
                continue

            paragraphs.append(self.process_paragraph(other.text))

        return paragraphs
