# https://codescience.wordpress.com/2011/02/06/python-mini-orm/
import sqlite3


class DataBase(object):

    def __init__(self, db):
        self.connection = self.get_sqlite_connection(db)
        self.cursor = self.db.cursor()

    def get_sqlite_connection(self, db=''):
        self.db = sqlite3.connect(db)

    def get_sqlite_columns(self, name):
        self.sql_rows = 'select * from %s' % name
        self.cursor.execute(self.sql_rows)
        return [row[1] for row in self.cursor.fetchall()]

    def Table(self, name):
        columns = self.providers[self.provider.__name__](name)
        return Query(self.cursor, self.sql_rows, columns, name)


class Query(object):

    def __init__(self, cursor, sql_rows, columns, name):
        self.cursor = cursor
        self.sql_rows = sql_rows
        self.columns = columns
        self.name = name

    def filter(self, criteria):
        key_word = "AND" if "WHERE" in self.sql_rows else "WHERE"
        sql = self.sql_rows + " %s %s" % (key_word, criteria)
        return Query(self.cursor, sql, self.columns, self.name)

    def order_by(self, criteria):
        return Query(self.cursor, self.sql_rows + " ORDER BY %s" % criteria, self.columns, self.name)

    def group_by(self, criteria):
        return Query(self.cursor, self.sql_rows + " GROUP BY %s" % criteria, self.columns, self.name)

    def get_rows(self):
        print(self.sql_rows)
        self.cursor.execute(self.sql_rows)
        return [Row(zip(self.columns, fields), self.name) for fields in self.cursor.fetchall()]

    rows = property(get_rows)


class Row(object):

    def __init__(self, fields, table_name):
        self.__class__.__name__ = table_name + "_Row"
        for name, value in fields:
            setattr(self, name, value)
