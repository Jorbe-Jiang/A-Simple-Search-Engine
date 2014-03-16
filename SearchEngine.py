#!/usr/bin/env python
# -*- coding: utf-8 -*-
import urllib2
from bs4 import BeautifulSoup   #网页文本解析
from urlparse import urljoin
import sqlite3  #数据库
from PorterStemmer import *    #词干提取
import CRFPP   #中文分词
import codecs
import sys   
import NeuralNetwork  #导入神经网络模块

neural_network=NeuralNetwork.SearchNet("db_network.db")


class Crawler(object):
	#初始化crawler类并传入数据库名称
	def __init__(self,dbname):
		self.conn=sqlite3.connect(dbname)
		self.stopwords=[]

	def __del__(self):
		self.conn.close()

	def dbcommit(self):
		self.conn.commit()
	
	#创建停用词列表
	def make_stopwords(self, stopwords_file="stopwords.txt"):
		for index,line in enumerate(open(stopwords_file,"rU")):    
                	line=line.strip()
	        	self.stopwords.append(line)

	#返回条目的id，并且如果条目不存在，就将其加入数据库
	def get_entry_id(self,table,field,value,create_new=True):
		cur=self.conn.execute("select rowid from %s where %s='%s'" %(table,field,value))
		res=cur.fetchone()  #返回结果行
		if res==None:
			cur=self.conn.execute("insert into %s(%s) values ('%s')" %(table,field,value))
			return cur.lastrowid
		else:
			return res[0]

	
	#为每个网页建立索引,将网页中的所有单词加入索引，建立网页和单词之间的关联
	def add_to_index(self,url,title,soup): 
		if self.is_indexed(url): return
		print "Indexing %s" %url
		#获取网页中的单词
		#text=self.get_text(soup)
		[script.extract() for script in soup.findAll('script')]
		[style.extract() for style in soup.findAll('style')]
		text=soup.get_text()
		text=unicode(text,'utf-8')
		#print text
		words=self.porter_stemmer(self.separate_words(text))  #分词，提取词干
		for w in words:
			w=w.encode('utf-8')
		print words
		#获取URL的id
		url_id=self.get_entry_id("urllist","url",url)
		#添加对应的url_title到urllist表中
		self.conn.execute("update urllist set url_title='%s' where rowid=%d" %(title,url_id))
		#讲每个单词与该url关联 
		for i in range(len(words)):
			word=words[i]
			if word in self.stopwords: continue
			word_id=self.get_entry_id("wordlist","word",word)
			self.conn.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" %(url_id,word_id,i))
		
	#从一个HTML网页中提取文字(去除标签),返回一个字符串
	def get_text(self,soup): 
		str=soup.string
		if str==None:
			content=soup.contents
			return_text=""
			for text in content:
				subtext=self.get_text(text)
				return_text+=subtext+"\n"
			return return_text
		else:
			return str.strip()
	
	#用CRF工具包进行中文分词，返回一个单词列表
	def crf_segmenter(self, text, tagger):
		str_text=""
		return_list=[]
		tagger.clear()
		for word in text.strip():
			word = word.strip()
			if word:
				tagger.add((word + "\to\tB").encode('utf-8'))
		tagger.parse()
		size = tagger.size()
		xsize = tagger.xsize()
		for i in range(0, size):
			for j in range(0, xsize):
				char = tagger.x(i, j)
				tag = tagger.y2(i)
				if tag == 'B':
					str_text+=(" " + char)
				elif tag == 'M':
					str_text+=char
				elif tag == 'E':
					str_text+=(char + " ")
				else: #tag == 'S'
					str_text+=(" " + char + " ")
		return_list=str_text.strip().split()
		return return_list
		
	#返回分好词的单词列表
	def separate_words(self,text):
		crf_model = "./crf_model"  #crf_model路径
		tagger = CRFPP.Tagger("-m " + crf_model)
		return_list=self.crf_segmenter(text,tagger)
		return return_list
	
	#词干提取(主要针对英文检索)，返回单词列表
	def porter_stemmer(self,words_list):
		p = PorterStemmer()
		return_list=[]
		for i in range(len(words_list)):
		    if words_list[i].isalpha():
				return_list.append(p.stem(words_list[i], 0,len(words_list[i])-1))
		    else:
				return_list.append(words_list[i])
		return return_list

	#判断网页是否已建立索引
	def is_indexed(self,url):
		u=self.conn.execute("select rowid from urllist where url ='%s'" %url).fetchone()
		if u!=None:
			#检查该网页是否已建立索引
			v=self.conn.execute("select * from wordlocation where urlid=%d" %u[0]).fetchone()
			if v!=None: return True
		return False

	#添加一个关联两个网页的链接
	def add_link_ref(self,url_from,url_to,link_text):
		link_text=unicode(link_text,'utf-8')
		words=self.porter_stemmer(self.separate_words(link_text))   #分词，提取词干
		for w in words:
			w=w.encode('utf-8')
		from_id=self.get_entry_id("urllist","url",url_from)
		to_id=self.get_entry_id("urllist","url",url_to)
		if from_id==to_id: return 
		cur=self.conn.execute("insert into link(fromid,toid) values (%d,%d)" %(from_id,to_id))
		link_id=cur.lastrowid
		for word in words:
			if word in self.stopwords: continue
			word_id=self.get_entry_id("wordlist","word",word)
			self.conn.execute("insert into linkwords(wordid,linkid) values (%d,%d)" %(word_id,link_id))
	
	#为urllist表中的url计算pagerank值，默认迭代次数为20，最后pr计算结果存入pagerank表
	def cal_pagerank(self,iterations=20):
		self.conn.execute("drop table if exists pagerank")
		self.conn.execute("create table pagerank(urlid primary key,score)")
		for (urlid,) in self.conn.execute("select rowid from urllist"):
			self.conn.execute("insert into pagerank(urlid,score) values (%d,1.0)" %urlid) #为每个url设置一个任意pr初始值1.0
		self.dbcommit()
		for i in range(iterations):
			print "Iteration %d" %i
			for (urlid,) in self.conn.execute("select rowid from urllist"):
				pr=0.15
				for (linker,) in self.conn.execute("select distinct fromid from link where toid=%d" %urlid):
					linking_pr=self.conn.execute("select score from pagerank where urlid=%d" %linker).fetchone()[0]
					linking_count=self.conn.execute("select count(*) from link where fromid=%d" %linker).fetchone()[0]
					pr+=0.85*(linking_pr/linking_count)
				self.conn.execute("update pagerank set score=%f where urlid=%d" %(pr,urlid))
			self.dbcommit()

	#从一小组网页开始进行广度优先搜索，直至某一给定深度，期间为网页建立索引
	def crawl(self,pages,depth=2):
		for i in range(depth):
			new_pages=set()  #存储pages中的所有未索引链接
			for page in pages:
				try:
					c=urllib2.urlopen(page)
				except:
					print "不能打开链接: %s" %page
					continue
				try:
					soup=BeautifulSoup(c.read())  #soup:一个网页的所有内容（包括标签属性）
					title=soup.html.head.title.string  #获取网页title标签里的字符串
					self.add_to_index(page,title,soup)  #为网页建立索引
					links=soup("a")
					print links
					for link in links:
						if("href" in dict(link.attrs)):
							url=urljoin(page,link["href"])
							if url.find("'")!=-1: continue
							url=url.split("#")[0]  #去掉位置部分
							if url[0:4]=="http" and not self.is_indexed(url):  #添加未索引的url
								new_pages.add(url)
							link_text=self.get_text(link)  #从HTML网页中提取文字内容（去除标签）
							self.add_link_ref(page,url,link_text) #把关联的两个url添加到相应数据库里
					self.dbcommit()
				except:
					print "不能解析该网页链接 %s" %page
			pages=new_pages

	#创建数据库表
	def create_index_tables(self):
		self.conn.execute("create table urllist(url,url_title)")
		self.conn.execute("create table wordlist(word)")
		self.conn.execute("create table wordlocation(urlid,wordid,location)")
		self.conn.execute("create table link(fromid integer,toid integer)")
		self.conn.execute("create table linkwords(wordid,linkid)")
		self.conn.execute("create index wordidx on wordlist(word)")
		self.conn.execute("create index urlidx on urllist(url)")
		self.conn.execute("create index wordurlidx on wordlocation(wordid)")
		self.conn.execute("create index urltoidx on link(toid)")
		self.conn.execute("create index urlfromidx on link(fromid)")
		self.dbcommit()

class Searcher(Crawler):
	def __init__(self,dbname):
		self.conn=sqlite3.connect(dbname)
		self.stopwords=[]

	def __del__(self):
		self.conn.close()

	#根据查询字符串从数据库wordlocation表中查找相应的urlid和location字段值，返回数据结果行列表和word_ids列表
	def get_match_rows(self,q):
		#构造查询的字符串
		field_list="w0.urlid"  #字段
		table_list=""  #表名
		clause_list=""  #子条件
		full_query=""  #完整查询语句
		word_ids=[]
		q=unicode(q,'utf-8')
		words=self.porter_stemmer(self.separate_words(q))
		for w in words:
			w=w.encode('utf-8')
		#print words
		table_number=0
		for word in words:
			word_row=self.conn.execute("select rowid from wordlist where word='%s'" %word).fetchone() 
			print word_row
			if word_row!=None:  #从后台数据库中匹配到结果
				word_id=word_row[0]
				word_ids.append(word_id)
				if table_number>0:
					table_list+=","
					clause_list+=" and "
					clause_list+="w%d.urlid=w%d.urlid and " %(table_number-1,table_number)
				field_list+=",w%d.location" %table_number
				table_list+="wordlocation w%d" %table_number
				clause_list+="w%d.wordid=%d" %(table_number,word_id)
				table_number+=1
		if(table_list.strip()!="" and clause_list.strip()!=""):
			full_query="select %s from %s where %s" %(field_list,table_list,clause_list)
		else:
			print "Sorry , Can't find the results!!!"  #若后台数据库中无法匹配
			return
		cur=self.conn.execute(full_query)
		rows=[row for row in cur]
		return rows,word_ids

	#归一化函数，默认为值越大排名越靠前，返回一个带有url_id和介于0到1之间的评价值的字典
	def normalize_scores(self,scores,small_is_better=0):
		vsmall=0.00001  #避免被零整除
		if small_is_better:
			min_score=min(scores.values())
			return dict([(u,float(min_score)/max(vsmall,v)) for (u,v) in scores.items()])
		else:
			max_score=max(scores.values())
			if max_score==0: max_score=vsmall
			return dict([(u,float(c)/max_score) for (u,c) in scores.items()])
	
	#按单词频度来计算排名，返回归一化值
	def frequency_score(self,rows):
		counts=dict([(row[0],0) for row in rows])
		for row in rows: counts[row[0]]+=1
		return self.normalize_scores(counts)  #归一化评价值
	
	#根据单词在文档中的出现位置进行排名，返回归一化值，单词在文档中的位置越靠前，权重越大
	def location_score(self,rows):
		locations=dict([(row[0],1000000) for row in rows])
		for row in rows:
			loc=sum(row[1:])
			if loc<locations[row[0]]: locations[row[0]]=loc
		return self.normalize_scores(locations,small_is_better=1) #值越小，排名越靠前
	
	#根据单词距离来进行排名，返回归一化值，单词距离越小，排名越靠前
	def distance_score(self,rows):
		if len(row[0])<=2: return dict([(row[0],1.0) for row in rows])
		min_distance=dict([(row[0],1000000) for row in rows])
		for row in rows:
			dist=sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
			if dist<min_distance[row[0]]: min_distance[row[0]]=dist
		return self.normalize_scores(min_distance,small_is_better=1)  #单词距离越小，排名越靠前
	
	#根据外部回指链接来进行排名，返回归一化值
	def inbound_link_score(self,rows):
		unique_urls=set([row[0] for row in rows])
		inbound_count=dict([(u,self.conn.execute("select count(*) from link where toid=%d" %u).fetchone()[0]) for u in unique_urls])
		return self.normalize_scores(inbound_count)

	#按pagerank值来进行排名，返回归一化分值
	def pagerank_score(self,rows):
		pageranks=dict([(row[0],self.conn.execute("select score from pagerank where urlid=%d" %row[0]).fetchone()[0]) for row in rows])
		max_rank=max(pageranks.values())
		normalized_scores=dict([(u,float(v)/max_rank) for (u,v) in pageranks.items()])
		return normalized_scores

	#利用链接文本来进行排名，返回归一化值
	def linktext_score(self,rows,word_ids):
		link_scores=dict([(row[0],0) for row in rows])
		for word_id in word_ids:
			cur=self.conn.execute("select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid" %word_id)
			for (fromid,toid) in cur:
				if toid in link_scores:
					pr=self.conn.execute("select score from pagerank where urlid=%d" %fromid).fetchone()[0]
					link_scores[toid]+=pr
		max_score=max(link_scores.values())
		normalized_scores=dict([(u,float(v)/max_score) for (u,v) in link_scores.items()])
		return normalized_scores

	#利用神经网络进行排名计算，返回归一化值
	def neuralnetwork_score(self,rows,word_ids):
		url_ids=[url_id for url_id in set([row[0] for row in rows])]
		res=neural_network.get_result(word_ids,url_ids)  #调用NeuralNetwork模块的get_result函数
		scores=dict([(url_ids[i],res[i]) for i in range(len(url_ids))])
		return self.normalize_scores(scores)

	#返回进过权重计算后的网页排名字典
	def get_scored_list(self,rows,word_ids):
		total_scores=dict([(row[0],0) for row in rows])	
		#weights=[(1.0,self.frequency_score(rows))]   #按单词频度来计算排名
		#weights=[(1.0,self.location_score(rows))]    #按单词在文档中的出现位置计算排名
		#weights=[(1.0,self.distance_score(rows))]   #按单词距离来计算排名
		#weights=[(1.0,self.inbound_link_score(rows))]   #按外部回指链接来计算排名
		#weights=[(1.0,self.pagerank_score(rows))]   #按pagerank值来计算排名
		#weights=[(1.0,self.linktext_score(rows,word_ids))]   #利用链接文本来计算排名
		#weights=[(1.0,self.neuralnetwork_score(rows,word_ids))]   #利用神经网络来计算排名
		weights=[(1.0,self.frequency_score(rows)),(1.0,self.location_score(rows)),(1.0,self.pagerank_score(rows)),(1.0,self.linktext_score(rows,word_ids)),(5.0,self.neuralnetwork_score(rows,word_ids))] #结合多种排名方式，按不同权重计算排名
		for (weight,scores) in weights:
			for url in total_scores:
				total_scores[url]+=weight*scores[url]
		return total_scores
	
	#根据url_id 从urllist表中查询相应的url，返回url
	def get_url_name(self,url_id):
		return self.conn.execute("select url from urllist where rowid=%d" %d).fetchone()[0]

	#根据url_id 从urllist表中查询相应的url_title，返回url_title
	def get_url_title(self,url_id):
		return self.conn.execute("select url_title from urllist where rowid=%d" %d).fetchone()[0]
	
	#主查询函数
	def query(self,q):
		rows,word_ids=self.get_match_rows(q)
		scores=self.get_scored_list(rows,word_ids)
		ranked_scores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
		for (score,url_id) in ranked_scores[0:20]:  #返回排名前10的网页
			print '%f\t <a href="%s" target="_blank">"%s"</a>' %(score,self.get_url_name(url_id),self.get_url_title(url_id))
		return word_ids,[r[1] for r in ranked_scores[0:10]]
	
