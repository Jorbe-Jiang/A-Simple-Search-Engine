A-Simple-Search-Engine
=======================

index.html--前端搜索界面

test.py--后台搜索

SearchEngine.py--Crawler类：爬虫程序 Searcher类：搜索程序

NeuralNetwork.py--神经网络

PorterStemmer.py--词干提取模块

stopwords.txt--停用词表

SearchEngine.py里的中文分词我用的是CRF++，当然也可以自己写个分词系统，但如果不想自己写分词系统的话，就需要在自己电脑上安装CRF++，并训练model，才能保证该程序正常运行。该文件里的separate_words()函数里的crf_model是我在自己电脑上训练出来的model。关于CRF++的安装和使用请看http://jorbe.sinaapp.com/2014/01/29/segment-sentences-with-crf/
