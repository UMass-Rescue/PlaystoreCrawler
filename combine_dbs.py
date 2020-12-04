import logging
import os
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlite3
from dateutil.parser import parse

Base = declarative_base()

class BundleId(Base):
    __tablename__ = 'bundle_ids'
    id = Column(Integer, primary_key=True)
    native_id = Column(String(250))

class App(Base):
    __tablename__ = 'apps'
    id = Column(Integer, primary_key=True)
    title = Column(String(250))
    bundle_id = Column(String(250), ForeignKey('bundle_ids.id'), sqlite_on_conflict_unique='IGNORE')
    url = Column(String(250))
    developer = Column(String(250))
    genre = Column(String(250))
    content_rating = Column(String(250))
    content_rating_description = Column(String(250))
    contains_ads = Column(Boolean)
    installs = Column(String(250))
    privacy_policy = Column(String(250))

class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    app_id = Column(Integer, ForeignKey('apps.id'))
    content = Column(String(500))
    review_native_id = Column(String(500), sqlite_on_conflict_unique='IGNORE')
    created_at = Column(DateTime)
    score = Column(Integer)
    reply_content = Column(String(500))

def init():
    engine = create_engine('sqlite:///combined.db')
    Base.metadata.create_all(engine)

    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn

def select_all(conn,session):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM apps")
    apps = cur.fetchall()

    for app in apps:
        a = App(
            title = app[1],
            bundle_id = app[2],
            url = app[3],
            developer = app[4],
            genre = app[5],
            content_rating = app[6],
            content_rating_description = app[7],
            contains_ads = app[8],
            installs = app[9],
            privacy_policy = app[10]
        )
        session.add(a)
        session.commit()
        session.refresh(a)

        cur = conn.cursor()
        cur.execute(f"SELECT * FROM reviews where app_id={a.id}")
        reviews = cur.fetchall()
        for r in reviews:
            r = Review(
                app_id = a.id,
                content = r[2],
                review_native_id = r[3],
                created_at = parse(r[4]),
                score = r[5],
                reply_content = r[6]
            )
            session.add(r)
            session.commit()

    cur = conn.cursor()
    cur.execute("SELECT * FROM bundle_ids")
    bundles = cur.fetchall()

    for b in bundles:
        b_id = BundleId(native_id=b[1])
        session.add(b_id)
        session.commit()

    cur = conn.cursor()
    cur.execute("SELECT * FROM bundle_ids")
    bundles = cur.fetchall()

if __name__ == '__main__':
    session =  init()
    conn = create_connection('reviews.db')
    select_all(conn, session)

    conn = create_connection('pof_reviews.db')
    select_all(conn, session)
