from google_play_scraper import app,Sort, reviews_all
from db import App, Review, BundleId, init
from tqdm import tqdm
from furl import furl
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger('ReviewScraperLogger')

def get_bundle_ids_from_inputs(csv_path, url_col):
    df = pd.read_csv(csv_path)
    ids = [furl(x.strip()).args.values()[0] for x in df[url_col]]
    logging.info(f'IDs to scan: {ids}')
    return ids

def persist_bundle_ids(session, ids):
    for id in ids:
        b_id = BundleId(native_id=id)
        session.add(b_id)
        session.commit()

def persist_reviews_for_bundle_ids(session):
    bundle_ids = session.query(BundleId).all()
    for index, bundle_id in tqdm(enumerate(bundle_ids)):
        try:
            app_data = app(bundle_id.native_id)
            app_row_id = persist_app_data(session, app_data)
            if app_row_id > 0:
                review_data = reviews_all(bundle_id.native_id, count=5)
                persist_review_data(session, app_row_id, review_data)
        except Exception as e:
            logging.exception(e)

def persist_app_data(session, app_data):
    apps = session.query(App).filter(App.bundle_id == app_data['appId']).all()
    if len(apps) < 1:
        a = App(
            title = app_data['title'],
            bundle_id = app_data['appId'],
            url = app_data['url'],
            developer = app_data['developer'],
            genre = app_data['genre'],
            content_rating = app_data['contentRating'],
            content_rating_description = app_data['contentRatingDescription'],
            contains_ads = app_data['containsAds'],
            installs = app_data['installs'],
            privacy_policy = app_data['privacyPolicy']
        )
        session.add(a)
        session.commit()
        session.refresh(a)
        return a.id
    return -1


def persist_review_data(session, app_row_id, review_data):
    for review in review_data:
        r = Review(
            app_id = app_row_id,
            content = review['content'],
            review_native_id = review['reviewId'],
            score = review['score'],
            reply_content = review['replyContent'],
            created_at = review['at']
        )
        session.add(r)
        session.commit()

def main():
    session = init(remove=False)
    ids = get_bundle_ids_from_inputs('inputs.txt','url')
    persist_bundle_ids(session, ids)
    persist_reviews_for_bundle_ids(session)


if __name__ == "__main__":
    main()
