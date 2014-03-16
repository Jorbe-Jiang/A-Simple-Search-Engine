#!/usr/bin/env python
# -*- coding: utf-8 -*-
from math import tanh
import sqlite3  #数据库

class SearchNet(object):
	def __init__(self,dbname):
		self.conn=sqlite3.connect(dbname)

	def __del__(self):
		self.conn.close()

	#创建数据库表
	def make_tables(self):
		self.conn.execute("create table hiddennode(create_key)")
		self.conn.execute("create table wordtohidden(fromid,toid,strength)")
		self.conn.execute("create table hiddentourl(fromid,toid,strength)")
		self.conn.commit()

	#判断当前连接的强度，并返回strength值
	def get_strength(self,fromid,toid,layer):
		if layer==0: table="wordtohidden"
		else: table="hiddentourl"
		res=self.conn.execute("select strenght from %s where fromid=%d and toid=%d" %(table,fromid,toid)).fetchone()
		if res==None:
			if layer==0: return -0.2  #单词层到隐藏层连接不存在时，设置默认值为-0.2
			if layer==1: return 0     #隐藏层到输出层连接不存在时，设置默认值为0
		return res[0]
	
	#设置或更新strength值
	def set_strength(self,fromid,toid,layer,strength):
		if layer==0: table="wordtohidden"
		else: table="hiddentourl"
		res=self.conn.execute("select rowid from %s where fromid=%d and toid=%d" %(table,fromid,toid)).fetchone()
		if res==None:
			self.conn.execute("insert into %s (fromid,toid,strength) values (%d,%d,%f)" %(table,fromid,toid,strength))
		else:
			rowid=res[0]
			self.conn.execute("update %s set strength=%f where rowid=%d" %(table,strength,rowid))
	
	#判断是否为单词建好了节点，若没有，则在隐藏层建立新节点，并设置默认权重
	def generate_hiddennode(self,word_ids,url_ids):
		if len(word_ids)>3: return None
		#检查是否为单词建好了节点
		create_key="_".join(sorted([str(wi) for wi in word_ids]))
		res=self.conn.execute("select rowid from hiddennode where create_key='%s'" %create_key).fetchone()
		#如果没有建好节点，则建立节点，并设置默认权重
		if res==None:
			cur=self.conn.execute("insert into hiddennode (create_key) values ('%s')" %create_key)
			hidden_id=cur.lastrowid
			for word_id in word_ids:
				self.set_strength(word_id,hidden_id,0,1.0/len(word_ids)) #设置默认权重为1.0/len(word_ids)
			for url_id in url_ids:
				self.set_strength(hidden_id,url_id,1,0.1)       #设置默认权重为0.1
			self.conn.commit()
	
	#获取与查询单词相关的所有隐藏层节点
	def get_all_hiddenids(self,word_ids,url_ids):
		res_dict={}
		for word_id in word_ids:
			cur=self.conn.execute("select toid from wordtohidden where fromid=%d" %word_id)
			for row in cur: res_dict[row[0]]=1
		for url_id in url_ids:
			cur=self.conn.execute("select fromid from hiddentourl where toid=%d" %url_id)
			for row in cur: res_dict[row[0]]=1
		return res_dict.keys()
	
	#建立神经网络
	def setup_network(self,word_ids,url_ids):
		self.word_ids=word_ids
		self.hidden_ids=self.get_all_hiddenids(word_ids,url_ids)
		self.url_ids=url_ids
		#节点输出
		self.a_input=[1.0]*len(self.word_ids)
		self.a_hidden=[1.0]*len(self.hidden_ids)
		self.a_output=[1.0]*len(self.url_ids)
		#建立权重矩阵
		self.wei_input=[[self.get_strength(word_id,hidden_id,0) for hidden_id in self.hidden_ids] for word_id in self.word_ids]
		self.wei_output=[[self.get_strength(hidden_id,url_id,1) for url_id in url_ids] for hidden_id in self.hidden_ids]

	#前馈算法，返回所有输出层节点的输出结果
	def feed_forward(self):
		for i in range(len(self.word_ids)):
			self.a_input[i]=1.0
		#隐藏层节点的活跃程度
		for j in range(len(self.hidden_ids)):
			sum=0.0
			for i in range(len(self.word_ids)):
				sum=sum+self.a_input[i]*self.wei_input[i][j]
			self.a_hidden[j]=tanh(sum)
		#输出层节点的活跃程度
		for k in range(len(self.url_ids)):
			sum=0.0
			for j in range(len(self.hidden_ids)):
				sum=sum+self.a_hidden[j]*self.wei_output[j][k]
			self.a_output[k]=tanh(sum)
		return self.a_output[:]

	#构建神经网络，并调用feed_forward函数，返回输出结果
	def get_result(self,word_ids,url_ids):
		self.setup_network(word_ids,url_ids)
		return self.feed_forward()

	#计算输出值在tanh函数曲线中的斜率
	def dtanh(y):
		return 1.0-y*y
		
	#反向传播法进行训练，不断更新输入、输出层的权重值
	def back_propagate(self,targets,rate=0.5):  #targets：期望结果 rate：学习速率
		#计算输出层的误差
		output_deltas=[0.0]*len(self.url_ids)
		for k in range(len(self.url_ids)):
			error=targets[k]-self.a_output[k]   #计算期望结果与输出结果差距
			output_deltas[k]=dtanh(self.a_output[k])*error
		#计算隐藏层的误差
		hidden_deltas=[0.0]*len(self.hidden_ids)
		for j in range(len(self.hidden_ids)):
			error=0.0
			for k in range(len(self.url_ids)):
				error=error+output_deltas[k]*self.wei_output[j][k]
			hidden_deltas[j]=dtanh(self.a_hidden[j])*error
		#更新输出权重
		for j in range(len(self.hidden_ids)):
			for k in range(len(self.url_ids)):
				change=output_deltas[k]*self.a_hidden[j]
				self.wei_output[j][k]=self.wei_output[j][k]+rate*change
		#更新输入权重
		for i in range(len(self.word_ids)):
			for j in range(len(self.hidden_ids)):
				change=hidden_deltas[j]*self.a_input[i]
				self.wei_input[i][j]=self.wei_input[i][j]+rate*change
	
	#更新数据库中权重值，权重信息位于实例变量wei_input和wei_output中
	def update_database(self):
		for i in range(len(self.word_ids)):
			for j in range(len(self.hidden_ids)):
				self.set_strength(self.word_ids[i],self.hidden_ids[j],0,self.wei_input[i][j])
		for j in range(len(self.hidden_ids)):
			for k in range(len(self.url_ids)):
				self.set_strength(self.hidden_ids[j],self.url_ids[k],1,self.wei_output[j][k])
		self.conn.commit()

	#训练实例
	def train_query(self,word_ids,url_ids):  #train_query(self,word_ids,url_ids,selected_url)
		self.generate_hiddennode(word_ids,url_ids)
		self.setup_network(word_ids,url_ids)  #建立神经网络
		self.feed_forward()  #运行前馈算法
		targets=[0.0]*len(url_ids)
		#targets[url_ids.index(selected_url)]=1.0
		self.back_propagate(targets)  #运行反向传播算法
		self.update_database()   #更新权重值
