#!/usr/bin/python
# -*- coding: utf-8 -*-
#一个简单的搜索测试页面
import cgi,os
import SearchEngine  #导入搜索引擎模块
import NeuralNetwork #导入神经网络模块

print "Content-type:text/html"
print

print """ <!DOCTYPE>
<HTML>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>A easy search engine</title>
</head>
<body>"""

form=cgi.FieldStorage()
q=cgi.escape(form["query"].value)
if form.has_key("query"):
	#爬去网页，建立索引
	crawler=SearchEngine.Crawler("db_search.db")
	#crawler.create_index_tables()  #运行一次即可
	crawler.make_stopwords(stopwords_file="stopwords.txt")
	pages=["http://book.douban.com/"]                 #预先准备好的url
	crawler.crawl(pages,depth=1)
	crawler.cal_pagerank(iterations=15)
	searcher=SearchEngine.Searcher("db_search.db")
	mynet=NeuralNetwork.SearchNet("db_network.db")
	#mynet.make_tables()   #运行一次即可
	mynet.train_query(searcher.query(q))
