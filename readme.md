# SecurityDateProcess

*Authors: 张泽恒*

SecurityDateProcess是有价证券数据预处理模块集合，支持从集思录网站下载可转债相关交易数据，支持从通达信下载股票相关交易数据，定义了有价证券通用的数据模型、可转债专用数据模型、k线交易数据模型，并定义了有相关模型的方法。数据持久化方面采用了mongodb nosql数据库，支持mongodb数据库的增删改查方法。

## Installation

直接将目录拷贝到项目目录下


## 文件结构

commonutils.py 定义了通用的函数方法
jisiluhelper.py 定义了从集思录网站下载可转债相关数据的类和方法
mootdxhelper.py 定义了从通达信网站下载股票相关数据的类和方法
SecurityModels.py 定义了通用有价证券、可转债、k线数据的mongodb模型

## 模块应用
1、从网站下载数据并存储到mongodb数据库中进行持久化存储，并支持到态更新

2、对可转债相关指标进行计算

3、定义了证券数据的查询集，从mongodb查询交易数据


