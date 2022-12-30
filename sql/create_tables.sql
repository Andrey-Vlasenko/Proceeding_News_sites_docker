-- Creation table of sources
CREATE TABLE IF NOT EXISTS sources ( 
  source_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_name VARCHAR NOT NULL,
  news_sources_main_urls VARCHAR,
  UNIQUE (source_name)
);

-- Creation table of new_categories
CREATE TABLE IF NOT EXISTS new_categories ( 
  cat_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  new_category_name VARCHAR NOT NULL,
  UNIQUE (new_category_name)
);

-- Creation table of old_categories
CREATE TABLE IF NOT EXISTS old_categories ( 
  cat_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  old_category_name VARCHAR NOT NULL,
  UNIQUE (old_category_name)
);

-- Creation table of old - new categories connections
CREATE TABLE IF NOT EXISTS category_changes ( 
  cat_change_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_name VARCHAR NOT NULL,
  old_category VARCHAR NOT NULL,
  new_category VARCHAR NOT NULL,
  CONSTRAINT fk_source
      FOREIGN KEY(source_name) 
	    REFERENCES sources(source_name),
  CONSTRAINT fk_cat_old
      FOREIGN KEY(old_category) 
	    REFERENCES old_categories(old_category_name),
  CONSTRAINT fk_cat1
      FOREIGN KEY(new_category) 
	    REFERENCES new_categories(new_category_name),
  UNIQUE (source_name, old_category, new_category)
);

-- Creation table of news
CREATE TABLE IF NOT EXISTS all_news ( 
  news_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, 
  source_name VARCHAR NOT NULL,
  date_of_publication TIMESTAMP NOT NULL,
  author VARCHAR,
  title VARCHAR NOT NULL,
  description VARCHAR,
  url_Link VARCHAR,
  category_0 VARCHAR NOT NULL,
  category_1 VARCHAR,
  category_2 VARCHAR,
  category_3 VARCHAR,
  category_4 VARCHAR,
  category_5 VARCHAR,
  category_6 VARCHAR,
  CONSTRAINT fk_source
      FOREIGN KEY(source_name) 
	    REFERENCES sources(source_name),
  CONSTRAINT fk_cat_old
      FOREIGN KEY(category_0) 
	    REFERENCES old_categories(old_category_name),
  UNIQUE (source_name, date_of_publication, title)
);

CREATE MATERIALIZED VIEW view_news_summary AS
(
WITH view_news AS 
(
	SELECT  n.source_name,
        n.category_0,
        COALESCE(c.new_category, 'Unknown...') AS category,
        CAST (n.date_of_publication AS date) AS publication_date, 
        EXTRACT(DOW FROM n.date_of_publication) AS publication_day,
        COUNT(*) AS total,
        CASE WHEN n.source_name = 'Лента' THEN 1 ELSE 0 END AS b_lenta,
        CASE WHEN n.source_name = 'Ведомости' THEN 1 ELSE 0 END AS b_vedomosti,
        CASE WHEN n.source_name = 'ТАСС' THEN 1 ELSE 0 END AS b_tass,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 0 
                    THEN 1 ELSE 0 END AS Sun,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 1 
                    THEN 1 ELSE 0 END AS Mon,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 2 
                    THEN 1 ELSE 0 END AS Tue,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 3 
                    THEN 1 ELSE 0 END AS Wed,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 4 
                    THEN 1 ELSE 0 END AS Thu,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 5 
                    THEN 1 ELSE 0 END AS Fri,
        CASE WHEN EXTRACT(DOW FROM n.date_of_publication) = 6 
                    THEN 1 ELSE 0 END AS Sat
    FROM all_news AS n
    LEFT JOIN category_changes as c
    ON c.source_name=n.source_name 
        AND c.old_category=n.category_0
    GROUP BY category, category_0, n.source_name, 
            publication_day, publication_date
),
last_day AS 
(
	SELECT 	category,
    		category_0 as old_category,
		    SUM(total) AS last_Total,
    		SUM(total*b_lenta) AS last_Total_Lenta,
		    SUM(total*b_vedomosti) AS last_Total_Vedomosti,
		    SUM(total*b_tass) AS last_Total_tass
    FROM view_news 
    WHERE publication_date = CURRENT_DATE-1
    GROUP BY category, category_0
),
cat_total AS 
(
	SELECT category,
    category_0  as old_category,
    SUM(total) AS Total,
    SUM(total*b_lenta) AS Total_Lenta,
    SUM(total*b_vedomosti) AS Total_Vedomosti,
    SUM(total*b_tass) AS Total_tass,
    SUM(total)/COUNT(DISTINCT publication_date) AS Average_News_Per_Day,
    SUM(total*Mon) AS Total_Mon,
    SUM(total*Tue) AS Total_Tue,
    SUM(total*Wed) AS Total_Wed,
    SUM(total*Thu) AS Total_Thu,
    SUM(total*Fri) AS Total_Fri,
    SUM(total*Sat) AS Total_Sat,
    SUM(total*Sun) AS Total_Sun
	FROM view_news
	GROUP BY category, old_category
),
max_day AS
(
	SELECT category, old_category, MAX(publication_date) as max_date
	FROM
    (	SELECT category,
    	category_0  AS old_category,
    	publication_date,
    	ROW_NUMBER() OVER(PARTITION BY category, category_0 
                        ORDER BY Total DESC) AS row_num
    	FROM view_news) as f
    WHERE row_num = 1
    GROUP BY category, old_category
) 
SELECT  t.category,
        t.old_category,
        Total,
        Total_Lenta,
        Total_Vedomosti,
        Total_tass,
        last_Total,
        last_Total_Lenta,
		last_Total_Vedomosti,
		last_Total_tass,
        Average_News_Per_Day,
        max_date,
        Total_Mon,
        Total_Tue,
        Total_Wed,
        Total_Thu,
        Total_Fri,
        Total_Sat,
        Total_Sun
FROM cat_total as t
LEFT JOIN last_day AS l
ON t.category=l.category AND t.old_category = l.old_category
LEFT JOIN max_day AS m
ON t.category=m.category AND t.old_category = m.old_category
);