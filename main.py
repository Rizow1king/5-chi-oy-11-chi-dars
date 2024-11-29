import psycopg2


class DataBase:
    def __init__(self):
        self.connect = psycopg2.connect(
            dbname='homework',
            user='postgres',
            host='localhost',
            password='1'
        )

    def manager(self, sql, *args, commit=False, fetchone=False, fetchall=False):
        with self.connect as db:
            with db.cursor() as cursor:
                cursor.execute(sql, args)
                if commit:
                    db.commit()
                if fetchone:
                    return cursor.fetchone()
                if fetchall:
                    return cursor.fetchall()

    def create_categories(self):
        sql = '''CREATE TABLE IF NOT EXISTS categories(
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT NOT NULL
        );'''
        self.manager(sql, commit=True)

    def create_news(self):
        sql = '''CREATE TABLE IF NOT EXISTS news(
            id SERIAL PRIMARY KEY,
            category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
            title VARCHAR(200),
            content TEXT NOT NULL,
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_published BOOLEAN DEFAULT FALSE
        );'''
        self.manager(sql, commit=True)

    def create_comments(self):
        sql = '''CREATE TABLE IF NOT EXISTS comments(
            id SERIAL PRIMARY KEY,
            news_id INTEGER REFERENCES news(id) ON DELETE SET NULL,
            author_name VARCHAR(100),
            comment_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );'''
        self.manager(sql, commit=True)

    def insert_categories(self):
        sql = '''INSERT INTO categories(name, description) VALUES
        ('Technology', 'There are too many technologies today'),
        ('Sports', 'Our sportsmen showed the best result this year'),
        ('Health', 'Nowadays there are several researchers exploring health topics');'''
        self.manager(sql, commit=True)

    def insert_news(self):
        sql = """INSERT INTO news (title, content, published_at, category_id) VALUES
        ('The Rise of Hate to R.Madrid', 'In the last games R.Madrid lost many games, leading to hate, but real fans stay loyal.', CURRENT_TIMESTAMP - INTERVAL '1 day', 1),
        ('Liverpool vs Real Madrid', 'Liverpool won 2-0 against Real Madrid.', CURRENT_TIMESTAMP, 2),
        ('Love', 'Love can improve mental and physical health.', CURRENT_TIMESTAMP - INTERVAL '2 day', 3);"""
        self.manager(sql, commit=True)

    def insert_comments(self):
        sql = """INSERT INTO comments (author_name, comment_text, created_at, news_id) VALUES
        ('Alijanov', 'The best', CURRENT_TIMESTAMP - INTERVAL '1 year', 1),
        ('Safobek', 'Nice', CURRENT_TIMESTAMP, 2),
        ('Sherdorbek', 'Good', CURRENT_TIMESTAMP, 3);"""
        self.manager(sql, commit=True)

    def update_news(self):
        sql = "UPDATE news SET views = 1;"
        self.manager(sql, commit=True)

    def update_news_days(self):
        sql = "UPDATE news SET is_published = True WHERE published_at < CURRENT_TIMESTAMP - INTERVAL '1 day';"
        self.manager(sql, commit=True)

    def delete_comments(self):
        sql = "DELETE FROM comments WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '1 year';"
        self.manager(sql, commit=True)

    def select_alias_news(self):
        sql = "SELECT news.id AS news_id, news.title AS news_title, categories.name AS category_name FROM news JOIN categories ON categories.id = news.category_id;"
        return self.manager(sql, fetchall=True)

    def select_technology(self):
        sql = "SELECT news.* FROM news JOIN categories ON categories.id = news.category_id WHERE name = 'Technology';"
        return self.manager(sql, fetchall=True)

    def select_is_published(self):
        sql = "SELECT * FROM news WHERE is_published = True ORDER BY published_at DESC LIMIT 5;"
        return self.manager(sql, fetchall=True)

    def select_views(self):
        sql = "SELECT id, title, content FROM news WHERE views BETWEEN 10 and 100;"
        return self.manager(sql, fetchall=True)

    def select_author_name(self):
        sql = "SELECT id, author_name, comment_text FROM comments WHERE author_name LIKE %s;"
        return self.manager(sql, 'A%', fetchall=True)

    def select_all_categories(self):
        sql = """
            SELECT categories.name AS name, COUNT(news.category_id) FROM categories LEFT JOIN news ON categories.id = news.category_id GROUP BY categories.name;
        """
        return self.manager(sql, fetchall=True)

    def unique_title(self):
        sql = "ALTER TABLE news ADD CONSTRAINT unique_title UNIQUE (title);"
        self.manager(sql, commit=True)


db = DataBase()
db = DataBase()

db.create_categories()
db.create_news()
db.create_comments()

db.insert_categories()
db.insert_news()
db.insert_comments()

print("Alias News:")
print(db.select_alias_news())

print("\nTechnology News:")
print(db.select_technology())

print("\nPublished News:")
print(db.select_is_published())

print("\nViews in range (10-100):")
print(db.select_views())

print("\nComments by authors starting with 'A':")
print(db.select_author_name('A%'))

print("\nAll Categories with news count:")
print(db.select_all_categories())

print("\nUpdating news views...")
db.update_news()
print("Views updated.")

print("\nPublishing news older than 1 day...")
db.update_news_days()
print("Old news published.")

print("\nDeleting old comments...")
db.delete_comments()
print("Old comments deleted.")

print("\nAdding unique constraint to news titles...")
db.unique_title()
print("Unique constraint added.")
