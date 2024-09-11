# PleaseNews

从GDELT数据库中获取数据，进行处理后生成SQLiteTest.db数据库文件。

## downcsv.py

下载GDELT数据库中的数据。包括GDELT1.0中的.exports.csv文件和GDELT2.0中的.mentions.csv文件，分别保存在./export和./mentions文件夹中。

### .exports.csv

记录了一天内报道的所有事件。一条记录代表一个事件，GlobalEventID是他的唯一标识符。一个事件可以由多条新闻分别报道，所有SourceURL字段只记录第一条报道该事件的新闻URL。

### .mentions.csv

记录的某一天的15分钟内的每一条新闻报道和一个事件的对应，一个新闻可以包含多个事件，一个事件也可以被多条新闻报道，所以GlobalEventID和MentionIdentifier均不唯一。

## Processcsv.py

### .merge.csv

 将同一天的.exports.csv和.mentions.csv按GlobalEventID进行内连接，合并两个文件，并进行排序、去重、添加唯一标识UniqueID等处理，产生.merge.csv文件。

### .media.merge.csv

从.merge.csv中筛选出在MEDIUMLIST列表中的新闻媒体发布的文章，保存在.media.merge.csv中。

##  crawlArticlesForDiffSourceURL.py

从.media.merge.csv中读取same_struct_domain_list列表中的所有媒体发布的文章的网址，爬取新闻。

./txt/DAY/unique_url_about_SOURCE_URL.txt 保存当天某一个媒体发布的所有新闻的网址

./txt/DAY/error_url_SOURCE_URL.txt 保存爬虫爬取失败的新闻网址

./articles/DAY/SOURCE_URL/UNIQUE_ID.txt 保存爬取的新闻标题、作者、日期、内容

> 爬取到的文章的发布日期与数据集中提供的日期可能不同，因为数据集中的日期是事件发生的日期，不是文章发布日期。

## processarticle.py

将crawlArticlesForDiffSourceURL.py爬取到的所有文章合并到一个csv文件中。

./pnews/DAY/SOURCE_URL.csv

./pnews/DAY/MentionSourceNames.csv

## ztest.py

读取./pnews/DAY/MentionSourceNames.csv，获取每篇文章的关键词，保存为Keywords_check.csv

