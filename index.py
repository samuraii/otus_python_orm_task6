from orm import DataBase

db = DataBase('database.db')
print(db.get_sqlite_columns('test'))