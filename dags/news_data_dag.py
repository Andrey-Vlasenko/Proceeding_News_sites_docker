from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import pickle
from joblib import dump, load
import sys
sys.path.append("./src/modules/")
from LoadData_fin import *
from sql_requests import *

"""
from sqlalchemy import create_engine
pg_hostname = 'host.docker.internal'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'postgres'
pg_db = 'news_db'
engine = create_engine('postgresql://'+pg_username+\
                           ':'+pg_pass+'@'+pg_hostname+':'+pg_port+'/'+pg_db)
"""

def load_data_to_psql(source_num : int = 0):
    """
    Combine data from csv files for selected source and load it to sql
    Parameters
    ----------
    source_num : int, optional
        Number of the news source (0-2). The default is 0.

    Returns
    -------
    None.

    """
 
    hook = PostgresHook(postgres_conn_id=Variable.get("connection_id"))
    engine = hook.get_sqlalchemy_engine()
    #source_num=0
    with engine.begin() as connection:
        if(source_num==0):
            last_time = connection.execute(SQL_get_max_time(0)).fetchone()[0]    
        elif(source_num==1):
            last_time = connection.execute(SQL_get_max_time(1)).fetchone()[0]
        elif(source_num==2):
            last_time = connection.execute(SQL_get_max_time(2)).fetchone()[0]
        else:
            last_time = None
        if last_time==None:
            news=combine_news(source_num)
        else:
            news=combine_news(source_num,start_date=last_time.astimezone())
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
            try:
                train=pd.DataFrame(columns=cols)
                train=pd.concat([train,
                                pd.DataFrame(connection.execute(SQL_get_resent_data())\
                                             .fetchall())])
                train = train.merge(cat_change,left_on=['source_name','category_0'],
                               right_on=['source_name','old_category'],
                               how='left')
                train = train[['title','new_category']]\
                    .append(news[['title','new_category']])
                train = train.dropna(subset=['title','new_category'])
                train  = train.drop_duplicates()
                pred = predict_category(news.title[news.new_category.isna()])#,
                                        #train.title, train.new_category)
                print(news.title[news.new_category.isna()],
                      news.category_0[news.new_category.isna()],
                      pred)
                pred=[c+'_' for c in pred]
                print(len(news.new_category.isna()),pred)
                news.new_category[news.new_category.isna()] = pred
            except:
                news.new_category[news.new_category.isna()] = "Unknown"
            
        news['category_6'] = news.new_category
        news = news.drop(["new_category"],axis=1)
        
        news = pd.concat([pd.DataFrame(columns=cols),news])[cols]
        if len(news.columns)>13 : 
            news = news[news.columns[:13]]
        print(news.groupby('category_6').category_6.count())
        news.category_0=news.category_0.fillna('Unknown')
        
        news.to_sql(f'tmp_news{source_num}', con=connection, 
                   if_exists='append', index=False)

def read_lenta_news():
    print("read lenta")
    load_news(0)

def read_vedomosti_news():
    print("read vedomosti")
    load_news(1)

def read_tass_news():
    print("read tass")
    load_news(2)

def load_lenta_news():
    print("load lenta")
    load_data_to_psql(0)

def load_vedomosti_news():
    print("load vedomosti")
    load_data_to_psql(1)

def load_tass_news():
    print("load tass")
    load_data_to_psql(2)
    
# Сбор данных и загрузка в БД Лента
with DAG(dag_id="collect_data_lenta", start_date=datetime(2022, 12, 24), 
         schedule="0,15,30,45 * * * *") as dag:

    # Tasks are represented as operators
    read_task = PythonOperator(task_id="read_from_web_to_csv", 
                               python_callable=read_lenta_news)
    load_to_psql_tsk = PythonOperator(task_id="load_from_csv_to_psql", 
                                      python_callable=load_lenta_news)
    update_final_data = PostgresOperator(
                                    task_id="update_data_in_sql",
                                    postgres_conn_id=Variable.get("connection_id"),
                                    sql=SQL_proceed_new_data(0))
    
    # Set dependencies between tasks
    read_task >> load_to_psql_tsk >> update_final_data

# Сбор данных и загрузка в БД Ведомости
with DAG(dag_id="collect_data_vedomosti", start_date=datetime(2022, 12, 24), 
         schedule="5,20,35,50 * * * *") as dag:

    # Tasks are represented as operators
    read_task = PythonOperator(task_id="read_from_web_to_csv", 
                               python_callable=read_vedomosti_news)
    load_to_psql_tsk = PythonOperator(task_id="load_from_csv_to_psql", 
                                      python_callable=load_vedomosti_news)
    update_final_data = PostgresOperator(
                                    task_id="update_data_in_sql",
                                    postgres_conn_id=Variable.get("connection_id"),
                                    sql=SQL_proceed_new_data(1))
    
    # Set dependencies between tasks
    read_task >> load_to_psql_tsk >> update_final_data

# Сбор данных и загрузка в БД ТАСС    
with DAG(dag_id="collect_data_tass", start_date=datetime(2022, 12, 24), 
         schedule="10,25,40,55 * * * *") as dag:

    # Tasks are represented as operators
    read_task = PythonOperator(task_id="read_from_web_to_csv", 
                               python_callable=read_tass_news)
    load_to_psql_tsk = PythonOperator(task_id="load_from_csv_to_psql", 
                                      python_callable=load_tass_news)
    update_final_data = PostgresOperator(
                                    task_id="update_data_in_sql",
                                    postgres_conn_id=Variable.get("connection_id"),
                                    sql=SQL_proceed_new_data(2))
    # Set dependencies between tasks
    read_task >> load_to_psql_tsk >> update_final_data

# Обновление витрины данных
with DAG(dag_id="refresh_data_view", start_date=datetime(2022, 12, 24), 
         schedule="0 4 * * *") as dag:
    # Tasks are represented as operators
    refresh_task = PostgresOperator(
        task_id="refresh_m_view",
        postgres_conn_id=Variable.get("connection_id"),
        sql="REFRESH MATERIALIZED VIEW view_news_summary;")    
    # Set dependencies between tasks
    refresh_task
