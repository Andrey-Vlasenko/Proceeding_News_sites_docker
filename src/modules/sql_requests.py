# -*- coding: utf-8 -*-
import sys
sys.path.append("./src/modules/")
from LoadData_fin import news_sources


def SQL_get_resent_data(days: int = 3):
    return f"""
    SELECT source_name, 
         title, 
         category_0
    FROM all_news
    WHERE date_of_publication > 
    CURRENT_DATE-{abs(days)}"""

def SQL_get_max_time(num_news_source : int = 0):
    return f"""
    SELECT MAX(date_of_publication)
    FROM all_news
    WHERE source_name='{news_sources[num_news_source]}'"""

def SQL_proceed_new_data(num_news_source : int = 0):
    return f"""
      DROP TABLE IF EXISTS new_vals{num_news_source};
      CREATE TABLE new_vals{num_news_source} AS 
      (
          SELECT source_name, old_category, new_category 
          FROM (SELECT t.source_name AS source_name, 
                     t.category_0 AS old_category, 
                     COALESCE(c.new_category, t.category_6) AS new_category, 
                     COUNT(*) as total, 
                     ROW_NUMBER() OVER(PARTITION BY t.source_name, t.category_0 ORDER BY COUNT(title) DESC) as row_num
                FROM tmp_news{num_news_source} AS t
                LEFT JOIN category_changes AS c
                ON t.category_0=c.old_category AND t.source_name = c.source_name
                WHERE c.new_category is NULL
                GROUP BY t.source_name, t.category_0,COALESCE(c.new_category, t.category_6) ) AS tab
          WHERE row_num = 1
      );
    
      INSERT INTO old_categories (old_category_name) 
      (SELECT DISTINCT old_category 
       FROM new_vals{num_news_source} 
       LEFT JOIN old_categories
       ON old_category_name=old_category 
       WHERE old_category_name IS NULL);
      
      INSERT INTO new_categories (new_category_name) 
      (SELECT DISTINCT new_category 
       FROM new_vals{num_news_source} 
       LEFT JOIN new_categories
       ON new_category_name=new_category 
       WHERE new_category_name IS NULL);
      
      INSERT INTO category_changes (source_name, old_category, new_category) 
      (SELECT source_name, old_category, new_category FROM new_vals{num_news_source});
      
      DROP TABLE new_vals{num_news_source};
      
      INSERT INTO all_news(source_name,date_of_publication,author,title,
    					description,url_link,category_0,category_1,
    					category_2,category_3,category_4,category_5,
    					category_6) 
          (SELECT DISTINCT t.source_name,
    		t.date_of_publication,
    		t.author,
    		t.title,
    		t.description,
    		t.url_link,
    		t.category_0,
    		t.category_1,
    		t.category_2,
    		t.category_3,
    		t.category_4,
    		t.category_5,
    		t.category_6
          FROM tmp_news{num_news_source} AS t
          LEFT JOIN all_news AS a
          ON t.title = a.title AND t.source_name = a.source_name
              AND t.date_of_publication = a.date_of_publication 
          WHERE a.title IS NULL);    
          
      TRUNCATE TABLE tmp_news{num_news_source};""";

SQL_get_max_times = """
SELECT source_name, 
       MAX(date_of_publication)
FROM all_news
GROUP BY source_name"""

SQL_get_max_time_lenta = """
SELECT MAX(date_of_publication)
FROM all_news
WHERE source_name='Лента'"""

SQL_get_max_time_vedomosti = """
SELECT MAX(date_of_publication)
FROM all_news
WHERE source_name='Ведомости'"""

SQL_get_max_time_tass = """
SELECT MAX(date_of_publication)
FROM all_news
WHERE source_name='ТАСС'"""
