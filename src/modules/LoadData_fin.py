# -*- coding: utf-8 -*-
"""
Модуль для сбора и обработки новостей с новостных сайтов 
(3 источника)
"""
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
import pickle
## библиотеки для определения категорий новостей - пока не работают в докер
try:
    from sklearn.feature_extraction.text import CountVectorizer
    from nltk.stem.snowball import SnowballStemmer
    from sklearn.neighbors import KNeighborsClassifier
except:
    print("No Modules sklearn in docker etc.")
    
# Названия источников
news_sources = ["Лента","Ведомости","ТАСС"]
    
# Ссылки для источников
news_sources_urls = {"Лента":"https://lenta.ru/rss/",
                     "Ведомости":"https://www.vedomosti.ru/rss/news",
                     "ТАСС":"https://tass.ru/rss/v2.xml"}

cols = ['source_name','date_of_publication','author','title',
        'description','url_link','category_0','category_1','category_2',
        'category_3','category_4','category_5','category_6']
Cols_Big = ['Source','Date_of_publication','Author','Title',
            'Description','URL_Link','Category_0','Category_1','Category_2',
            'Category_3','Category_4','Category_5','Category_6']

def load_news(num_news_source : int = 0, 
              start_date : datetime = datetime(2000, 1, 1, tzinfo=pytz.UTC),
              data_path : str = "./data/"):
    """
    Collect news table data from selected source.

    Parameters
    ----------
    num_news_source : int, optional 
        Selected number of the news source to collect the news 
        Sources are 0:"Лента", 1:"Ведомости", 2:"ТАСС". 
        The default is 0.
    start_date : datetime, optional
        Start date to collect news from the source. 
        The default is datetime(2000, 1, 1, tzinfo=pytz.UTC).
    data_path : str, optional
        Path to save data files. The default is "./data/".

    Returns
    -------
    result : list of dictionaries
        Returns collected news data - list of articles. 
    """
    # Интересуемые поля тэг : название переменной
    fields = {'pubDate':'Date_of_publication',
              'author':'Author',
              'title':'Title',
              'description':'Description',
              'link':'URL_Link',
              'category':'Category'}
    
    news_source=news_sources[num_news_source]
    url = news_sources_urls[news_source]
    r=requests.get(url,timeout=10)#, 
#                   headers={'User-Agent':
#                            generate_user_agent(device_type="desktop",
#                                                os=('mac', 'linux'))})
    page_content = BeautifulSoup(r.content, "xml")
    
    # Получаем список новостей
    news_on_page = page_content.find_all('item')
    
    result = []
    bStopProcessNews = False
    for news_item in news_on_page:
        tmp_str = {"Source":news_source}
        for tag,col_name in fields.items():
            #находим значения тегов
            tmp_values  = news_item.find_all(tag)
            # Обработка полей новости
            if tag == 'pubDate':
                try:
                    tmp_str[col_name]=datetime.strptime(tmp_values[0].text,
                                                        '%a, %d %b %Y %X %z')
                    if tmp_str[col_name] <= start_date:
                        bStopProcessNews = True
                        break
                except:
                    tmp_str[col_name]=float("NaN")
                    break
            elif tag == 'link':
                try:
                    tmp_str[col_name]=tmp_values[0].text
                    tmp_str[col_name]=re.sub('[\n\t\r ]','',
                                             tmp_str[col_name])
                except:
                    None
                    tmp_str[col_name]=float("NaN")
            elif tag == 'category':
                # В одном из источников несколько полей со значением категория
                if len(tmp_values)>0:
                    for i in range(len(tmp_values)):
                        fin_name=col_name+'_'+str(i)
                        tmp_str[fin_name]=tmp_values[i].text
                else:
                    tmp_str[col_name+'_0']=float("NaN")
            else:
                try:
                    tmp_str[col_name]=tmp_values[0].text
                    tmp_str[col_name]=re.sub('[\n\t\r]',' ',
                                             tmp_str[col_name])
                except:
                    None
                    tmp_str[col_name]=float("NaN")
        if bStopProcessNews:
            break
        result.append(tmp_str)
    if len(result)>0:
        news = pd.DataFrame(result)
        news.to_csv(data_path+'news_'+\
                    news.Source[0]+'_'+\
                        max(news.Date_of_publication)\
                            .strftime("%y%m%d_%H%M%S%z")+'.csv.gz',
                    compression='gzip',index=False)
    return result


def get_last_time(num_news_source : int = 0, 
                  data_path : str = "./data/"):
    """
    Returns the time of the last saved news for the data source.
    Parameters
    ----------
    Selected number of the news source to collect the news 
        Sources are 0:"Лента", 1:"Ведомости", 2:"ТАСС". 
        The default is 0.
    data_path : str, optional
        Path to saved data files. The default is "./data/".

    Returns
    -------
    last_datetime : datetime
        last time of saved news in selected source.
    """
    # получаем время последней новости из источника
    news_source=news_sources[num_news_source]
    Mask = 'news_'+news_source+'[_0-9+]{1,}'+".csv.gz"
    CurFileNames=[f for f in os.listdir(data_path) if re.match(Mask, f)]
    news_dates=[re.sub(".csv.gz"+'*','',
                           re.sub('news_'+news_source+'_','',f))
                    for f in CurFileNames]
    try:
        last_datetime = max(news_dates)    
        last_datetime = datetime.strptime(last_datetime,"%y%m%d_%H%M%S%z")
    except:
        last_datetime = datetime(2000, 1, 1, tzinfo=pytz.UTC)
    print("Source: ",news_source, " \tLast news:",
          last_datetime.strftime("%y%m%d_%H%M%S%z"))
    return last_datetime


def combine_news(num_news_source : int = 0, 
              start_date : datetime = datetime(2000, 1, 1, tzinfo=pytz.UTC),
              data_path : str = "./data/"):
    """
    Combine news tables for selected source from files in selected path.

    Parameters
    ----------
    num_news_source : int, optional 
        Selected number of the news source to collect the news 
        Sources are 0:"Лента", 1:"Ведомости", 2:"ТАСС". 
        The default is 0.
    start_date : datetime, optional
        Start date to combine news for the source. 
        The default is datetime(2000, 1, 1, tzinfo=pytz.UTC).
    data_path : str, optional
        Path to saved data files. The default is "./data/".

    Returns
    -------
    result : pd.DataFrame
        Returns combined news tables. 
    """
    i=0
    news_source=news_sources[num_news_source]
    Mask = 'news_'+news_source+'[_0-9+]{1,}'+".csv.gz"
    CurFileNames=[f for f in os.listdir(data_path) if re.match(Mask, f)]
    news_dates=[re.sub(".csv.gz"+'*','',
                           re.sub('news_'+news_source+'_','',f))
                    for f in CurFileNames]
    result=pd.DataFrame(columns=Cols_Big)
    
    date = news_dates[0]
    for date in news_dates:
        try:
            if (start_date <= datetime.strptime(date,"%y%m%d_%H%M%S%z")):
                tmp_data = pd.read_csv(data_path+'news_'+news_source+\
                               '_'+date+'.csv.gz',
                               compression='gzip')
                result = pd.concat([result,tmp_data])
                i+=1
        except:
            print("error with ",data_path+'news_'+news_source+\
                               '_'+date+'.csv.gz')
    result.drop_duplicates().reset_index(drop=True)
    result['Date_of_publication'] = \
        [datetime.strptime(c,'%Y-%m-%d %H:%M:%S%z') \
         for c in result.Date_of_publication]
    print(i," files loaded.")
    return result

def predict_category(new_titles,titles="",categories=""):
    """
    Function to predict categories of news for new titles of news.
    
    Parameters
    ----------
    new_titles : new list of strings to predict category.
    titles : list of strings with descriptions (X)
    categories : list of strings (classes) (y).
    
    Returns
    -------
    list of strings - predicted classes
    """
    try:
        if titles != "" and categories != "":
            stemmer = SnowballStemmer("russian")
            analyzer = CountVectorizer(max_df=0.999,min_df=0.001).build_analyzer()
            def stemmed_words(doc):
                return (stemmer.stem(w) for w in analyzer(doc))
            stem_vectorizer = CountVectorizer(analyzer=stemmed_words)
            X = stem_vectorizer.fit_transform(titles)
            y = categories
            neigh = KNeighborsClassifier(n_neighbors=5)
            neigh.fit(X,y)
            X = stem_vectorizer.transform(new_titles)
            y1=neigh.predict(X)
        else:
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
            y1=neigh.predict(stem.transform(titles))
    except:
        y1="Unknown"
    return y1
    
if __name__ == '__main__':

    news0=load_news(0,get_last_time(0,"./src/tests_data/"),"./src/tests_data/")
    news1=load_news(1,get_last_time(1,"./src/tests_data/"),"./src/tests_data/")
    news2=load_news(2,get_last_time(2,"./src/tests_data/"),"./src/tests_data/")
    print(len(news0),len(news1),len(news2))
    
    get_last_time(0,"./src/tests_data/")
    get_last_time(1,"./src/tests_data/")
    get_last_time(2,"./src/tests_data/")
    
    news0=combine_news(0,data_path="./src/tests_data/")
    news1=combine_news(1,data_path="./src/tests_data/")
    news2=combine_news(2,data_path="./src/tests_data/")
    news = pd.concat([news0,news1,news2])
    news=news.dropna(subset=['Title'])
    news=news.drop_duplicates()
    news.to_csv('./src/tests_data/news_All.csv',index=False)  
