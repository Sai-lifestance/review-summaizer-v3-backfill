import datetime
from app.clients import bq_client
from app.config import BQ_PROJECT_REVIEWS, REVIEWS_TABLE
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def last_complete_fri_to_thu(today=None):
    """Return the most recent *completed* Friday–Thursday date range."""
    if today is None:
        today = datetime.date.today()

    weekday = today.weekday()  # Monday=0, Sunday=6

    # Find last Thursday (the most recent Thursday before today)
    # If today is Friday (4), last Thursday was yesterday
    # If today is Thursday, go back one week to last Thursday
    if weekday >= 4:  # Friday–Sunday
        days_since_thu = weekday - 3
    else:  # Monday–Thursday
        days_since_thu = weekday + 4  # wrap to previous week

    last_thu = today - datetime.timedelta(days=days_since_thu)
    last_fri = last_thu - datetime.timedelta(days=6)

    return last_fri.isoformat(), last_thu.isoformat()

def get_reviews(start_date, end_date):

    query = f"""
    SELECT date, review_rating, review_comment
    FROM `{BQ_PROJECT_REVIEWS}.{REVIEWS_TABLE}`
    WHERE review_source = "Google" 
        AND date between "{start_date}" and "{end_date}"
    """

    logger.info(f"Retrieving reviews for {start_date} to {end_date}.")
    
    query_job = bq_client.query(query)
    results = query_job.result()

    return [dict(row) for row in results]

def load_review_categories(file_path: str="data/review_keywords.csv") -> dict:

    logger.info("Loading review categories for sentiment grader.")

    df = pd.read_csv(file_path)
    df["keywords"] = df["keywords"].apply(lambda x: [kw.strip() for kw in str(x).split(",")])

    return dict(zip(df["category"], df["keywords"]))