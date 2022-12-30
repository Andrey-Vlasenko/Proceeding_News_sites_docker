INSERT INTO sources (source_name, news_sources_main_urls) 
VALUES 
('Лента','https://lenta.ru/rss/'),
('Ведомости','https://www.vedomosti.ru/rss/news'),
('ТАСС','https://tass.ru/rss/v2.xml');

INSERT INTO old_categories (old_category_name) 
VALUES 
('Моя страна'),
('Интернет и СМИ'),
('Среда обитания'),
('Мир'),
('Экономика'),
('Бывший СССР'),
('Ценности'),
('Забота о себе'),
('Путешествия'),
('Спорт'),
('Культура'),
('Россия'),
('Силовые структуры'),
('Наука и техника'),
('Из жизни'),
('Оружие'),
('69-я параллель'),
('Политика'),
('Общество'),
('Технологии'),
('Бизнес'),
('Финансы'),
('Авто'),
('Медиа'),
('Недвижимость'),
('Новости партнеров'),
('Экономика и бизнес'),
('Международная панорама'),
('Происшествия'),
('Армия и ОПК'),
('Москва'),
('Новости регионов'),
('Наука'),
('Космос'),
('Сибирь'),
('Биографии и справки'),
('Северный Кавказ'),
('Новости Урала'),
('В стране'),
('Северо-Запад'),
('Московская область'),
('Транспорт'),
('Внешняя политика'),
('Внутренняя политика'),
('Военная операция на Украине'),
('Летние виды спорта'),
('Футбол'),
('Новый год и Рождество'),
('Киберспорт'),
('АПК'),
('Образование'),
('Ситуация с криптовалютой в России'),
('Искусство'),
('Санкции в отношении России'),
('Стрельба в школе в Ижевске'),
('Зимние виды спорта'),
('Дело Саакашвили'),
('Опросы общественного мнения'),
('Ситуация в зоне нагорнокарабахского конфликта'),
('Пандемия COVID-19'),
('Кино'),
('Реновация'),
('Массовый спорт'),
('Криминал'),
('Национальные проекты'),
('Хоккей'),
('Нагорно-карабахский конфликт'),
('Ситуация вокруг ядерной программы Ирана'),
('Ситуация на Корейском полуострове'),
('Чемпионат России по фигурному катанию'),
('Частичная мобилизация в России'),
('Колебания курса рубля'),
('Ситуация с поставками энергоносителей из РФ в Европу'),
('ТАСС на МКС'),
('Ракетные запуски в КНДР'),
('Эпидемия гриппа в России'),
('Беседы с Иваном Сурвилло'),
('Арктика'),
('Туризм и отдых'),
('Мировые цены на нефть'),
('Музыка'),
('Стихийные бедствия'),
('Протесты сторонников Трампа в США'),
('Дальний Восток'),
('Покушение на главу Русского дома в ЦАР'),
('Распространение свиного гриппа в России'),
('Теннис'),
('Форум "Россия - спортивная держава"'),
('Стрельба в Пермском государственном университете'),
('Летние неолимпийские виды спорта'),
('Сборные по футболу'),
('ЧМ-2022 по футболу'),
('Шахматы'),
('Зимние олимпийские виды спорта'),
('Футбол в России'),
('Туризм в России'),
('Сборные по хоккею'),
('Олимпиада-2024 в Париже'),
('Хоккей с мячом'),
('Летние олимпийские виды спорта'),
('Экология'),
('Кузбасс'),
('КХЛ'),
('Премия "Оскар"'),
('НХЛ'),
('Мировой футбол'),
('Безопасные качественные дороги'),
('Футбол в Англии'),
('Республика Саха (Якутия)'),
('Здоровье'),
('Нижегородская область'),
('WTA'),
('Расследование утечек газа из "Северных потоков"'),
('Паводок в Иркутской области'),
('Жилье и городская среда'),
('Демография');


INSERT INTO new_categories (new_category_name) 
VALUES 
('Новости РФ'),
('Разное'),
('Армия и ОПК'),
('Экономика Финансы Бизнес'),
('В мире'),
('Здоровье и спорт'),
('Культура, общество и медиа'),
('Наука и техника');

INSERT INTO category_changes (source_name, old_category, new_category) 
VALUES
('Ведомости','Авто','Разное'),
('Ведомости','Бизнес','Экономика Финансы Бизнес'),
('Ведомости','Медиа','Культура, общество и медиа'),
('Ведомости','Недвижимость','Разное'),
('Ведомости','Общество','Культура, общество и медиа'),
('Ведомости','Политика','В мире'),
('Ведомости','Технологии','Наука и техника'),
('Ведомости','Финансы','Экономика Финансы Бизнес'),
('Ведомости','Экономика','Экономика Финансы Бизнес'),
('Лента','69-я параллель','Новости РФ'),
('Лента','Бывший СССР','В мире'),
('Лента','Забота о себе','Здоровье и спорт'),
('Лента','Из жизни','Культура, общество и медиа'),
('Лента','Интернет и СМИ','Культура, общество и медиа'),
('Лента','Культура','Культура, общество и медиа'),
('Лента','Мир','В мире'),
('Лента','Моя страна','Новости РФ'),
('Лента','Наука и техника','Наука и техника'),
('Лента','Оружие','Армия и ОПК'),
('Лента','Путешествия','В мире'),
('Лента','Россия','Новости РФ'),
('Лента','Силовые структуры','Армия и ОПК'),
('Лента','Спорт','Здоровье и спорт'),
('Лента','Среда обитания','Здоровье и спорт'),
('Лента','Ценности','Разное'),
('Лента','Экономика','Экономика Финансы Бизнес'),
('ТАСС','Армия и ОПК','Армия и ОПК'),
('ТАСС','Биографии и справки','Разное'),
('ТАСС','В стране','Новости РФ'),
('ТАСС','Космос','Наука и техника'),
('ТАСС','Культура','Культура, общество и медиа'),
('ТАСС','Международная панорама','В мире'),
('ТАСС','Москва','Новости РФ'),
('ТАСС','Московская область','Новости РФ'),
('ТАСС','Наука','Наука и техника'),
('ТАСС','Недвижимость','Разное'),
('ТАСС','Новости партнеров','Разное'),
('ТАСС','Новости регионов','Новости РФ'),
('ТАСС','Новости Урала','Новости РФ'),
('ТАСС','Общество','Культура, общество и медиа'),
('ТАСС','Политика','В мире'),
('ТАСС','Происшествия','Разное'),
('ТАСС','Северный Кавказ','Новости РФ'),
('ТАСС','Северо-Запад','Новости РФ'),
('ТАСС','Сибирь','Новости РФ'),
('ТАСС','Спорт','Здоровье и спорт'),
('ТАСС','Экономика и бизнес','Экономика Финансы Бизнес');