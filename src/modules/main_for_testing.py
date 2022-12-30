from sqlalchemy import create_engine
import sys
sys.path.append("./src/modules/")
from LoadData_fin import *
from sql_requests import *

from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem.snowball import SnowballStemmer
from sklearn.neighbors import KNeighborsClassifier
try:
    import ferfdsf
except:
    print(1)

pg_hostname = 'host.docker.internal'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'postgres'
pg_db = 'news_db'

news0=load_news(0,get_last_time(0,"./src/tests_data/"),"./src/tests_data/")
news1=load_news(1,get_last_time(1,"./src/tests_data/"),"./src/tests_data/")
news2=load_news(2,get_last_time(2,"./src/tests_data/"),"./src/tests_data/")
print(len(news0),len(news1),len(news2))
    
get_last_time(0,"./src/tests_data/")
get_last_time(1,"./src/tests_data/")
get_last_time(2,"./src/tests_data/")
    


news_source=news_sources[2]
Mask = 'news_'+news_source+'[_0-9+]{1,}'+".csv.gz"
data_path="./src/tests_data/"
CurFileNames=[f for f in os.listdir(data_path) if re.match(Mask, f)]
news_dates=[re.sub(".csv.gz"+'*','',
                       re.sub('news_'+news_source+'_','',f))
                for f in CurFileNames]
print(len(news_dates))

news0=combine_news(0,data_path="./src/tests_data/").drop_duplicates()
news1=combine_news(1,data_path="./src/tests_data/").drop_duplicates()
news2=combine_news(2,data_path="./src/tests_data/").drop_duplicates()
news = pd.concat([news0,news1,news2])
news=news.dropna(subset=['Title'])
news=news.drop_duplicates()
news.columns = [str(c).lower().replace('source','source_name') \
                for c in news.columns]
news=news.drop_duplicates(['source_name','title','date_of_publication'])



engine = create_engine('postgresql://'+pg_username+\
                       ':'+pg_pass+'@'+pg_hostname+':'+pg_port+'/'+pg_db)


with engine.begin() as connection:
    last_time = connection.execute(SQL_get_max_time_lenta).fetchone()[0]   
    if last_time==(None,):
        last_time=datetime(2000, 1, 1, tzinfo=pytz.UTC)
    news=combine_news(0,start_date=last_time.astimezone(),
                      data_path="./src/tests_data/")
    news=news.dropna(subset=['Title'])
    news=news.drop_duplicates()
    news.columns = [str(c).lower().replace('source','source_name') \
                    for c in news.columns]
    news=news.drop_duplicates(['source_name','title','date_of_publication'])
    
               
    cat_change = pd.read_sql('category_changes', con=connection)
    news = news.merge(cat_change,left_on=['source_name','category_0'], 
                   right_on=['source_name','old_category'],
                   how='left').drop(["old_category"],axis=1)
    
    if(sum(news.new_category.isna())>0):
        train=pd.DataFrame(connection.execute(get_resent_data).fetchall())
        train = train.merge(cat_change,left_on=['source_name','category_0'],
                       right_on=['source_name','old_category'],
                       how='left')
        train = train[['title','new_category']].append(news[['title','new_category']])
        train = train.dropna(subset=['title','new_category'])
        train  = train.drop_duplicates()
        pred = predict_category(train.title, train.new_category,
                         news.title[news.new_category.isna()])
        print(news.title[news.new_category.isna()],
              news.category_0[news.new_category.isna()],
              pred)
        pred=[c+'_' for c in pred]
        news.new_category[news.new_category.isna()] = pred

    news['category_6'] = news.new_category
    news = news.drop(["new_category"],axis=1)
    
    if len(news.columns)>13 : 
        news = news[news.columns[:13]]
    news.to_sql('tmp_news', con=connection, 
               if_exists='append', index=False)
    connection.execute(SQL_proceed_new_data)
    

    
   