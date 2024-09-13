from crawlingnews.NULL.crawlArticlesForDiffSourceURL import crawl_days
from downcsv import download_csv
from helper.SQLALchemyUtil import to_sql
from processarticle import process_articles
from processcsv import process_csv
from zlog.ztest import get_keywords

if __name__ == '__main__':
    # download_csv()
    # process_csv()
    crawl_days()
    process_articles()
    get_keywords()
    to_sql()
