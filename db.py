import logging
import os
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

def init(remove=False):
    if remove:
        try:
            os.remove('reviews.db')
        except Exception as e:
            logging.exception(e)

    if not os.path.exists('reviews.db'):
        engine = create_engine('sqlite:///reviews.db')
        Base.metadata.create_all(engine)

    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session
