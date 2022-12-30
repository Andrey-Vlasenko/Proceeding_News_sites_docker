# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 19:25:15 2022

@author: andre
"""
import pickle
from sqlalchemy import create_engine
import sys
sys.path.append("./src/modules/")
from LoadData_fin import *
from sql_requests import *


pg_hostname = 'host.docker.internal'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'postgres'
pg_db = 'news_db'
engine = create_engine('postgresql://'+pg_username+\
                           ':'+pg_pass+'@'+pg_hostname+':'+pg_port+'/'+pg_db)
# Загружаем нужные библиотеки
## библиотеки работы с датами
from datetime import datetime
import pytz
## библиотеки для загрузки данных из интернета
import requests
from bs4 import BeautifulSoup
import re
## библиотеки для обратоки и сохранения данных
import pandas as pd
#from user_agent import generate_user_agent #- не захотел устанавливаться в докер
import os
## библиотеки для определения категорий новостей - пока не работают в докер
try:
    from sklearn.feature_extraction.text import CountVectorizer
    from nltk.stem.snowball import SnowballStemmer
    from sklearn.neighbors import KNeighborsClassifier
except:
    print("No Modules sklearn in docker etc.")
    
if __name__ == '__main__':
    
    news0=combine_news(0,data_path="./data/")
    news1=combine_news(1,data_path="./data/")
    news2=combine_news(2,data_path="./data/")
    
    news = pd.concat([news0,news1,news2])
    news=news.dropna(subset=['Title'])
    news=news.drop_duplicates()
    news.columns = [str(c).lower().replace('source','source_name') \
                        for c in news.columns]
    news=news.drop_duplicates(['source_name','title','date_of_publication'])

    with engine.begin() as connection:
        cat_change = pd.read_sql('category_changes', con=connection)
    
    cat_change = cat_change.iloc[list(cat_change['new_category']!='Unknown'),1:]
    news = news.merge(cat_change,left_on=['source_name','category_0'], 
                       right_on=['source_name','old_category'],
                       how='left').drop(["old_category"],axis=1)
    
    news=news.iloc[list(news['new_category'].isna()==False),:].drop_duplicates()
    
    titles = news.title
    categories = news.new_category
    
    stemmer = SnowballStemmer("russian")
    analyzer = CountVectorizer(max_df=0.999,min_df=0.001).build_analyzer()
        
    def stemmed_words(doc):
        return (stemmer.stem(w) for w in analyzer(doc))
        
    stem_vectorizer = CountVectorizer(analyzer=stemmed_words)
    
    X = stem_vectorizer.fit_transform(titles)
    y = categories
        
    neigh = KNeighborsClassifier(n_neighbors=7)
    neigh.fit(X,y)
                    
    with open('./src/modules/model_knn.pkl', 'wb') as model_pkl:
        pickle.dump(neigh, model_pkl)
    with open('./src/modules/model_stem.pkl', 'wb') as model_pkl:
        pickle.dump(stem_vectorizer, model_pkl)
    
    sum(neigh.predict(X)==y)
        
    model = []
    with (open('./src/modules/model_knn.pkl', 'rb')) as openfile:
        while True:
            try:
                model.append(pickle.load(openfile))
            except EOFError:
                break
    neigh=model[0]
    model = []
    with (open('./src/modules/model_stem.pkl', 'rb')) as openfile:
        while True:
            try:
                model.append(pickle.load(openfile))
            except EOFError:
                break
    stem=model[0]
    sum(neigh.predict(stem.transform(titles))==y)
