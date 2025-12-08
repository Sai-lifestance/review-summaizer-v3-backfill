# app/utils.py

import datetime
from app.clients import bq_client
from app.config import BQ_PROJECT_REVIEWS, REVIEWS_TABLE
import pandas as pd
import logging
from pathlib import Path  # 👈 add this

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def last_complete_fri_to_thu(today=None):
    if today is None:
        today = datetime.date.today()
    weekday = today.weekday()  # Mon=0..Sun=6
    if weekday >= 4:  # Fri–Sun
        days_since_thu = weekday - 3
    else:            # Mon–Thu
        days_since_thu = weekday + 4
    last_thu = today - datetime.timedelta(days=days_since_thu)
    last_fri = last_thu - datetime.timedelta(days=6)
    return last_fri.isoformat(), last_thu.isoformat()

def get_reviews(start_date, end_date):
    query = f"""
    SELECT
        date,
        primary_key,   
        review_rating,
        review_comment
    FROM `{BQ_PROJECT_REVIEWS}.{REVIEWS_TABLE}`
    WHERE review_source = 'Google'
      AND date BETWEEN '{start_date}' AND '{end_date}'
    """
    logger.info(f"Retrieving reviews for {start_date} to {end_date}.")
    return [dict(row) for row in bq_client.query(query).result()]

def load_review_categories(file_path: str = None) -> dict:
    # Resolve <repo_root>/data/review_keywords.csv unless an explicit path is given
    csv_path = (
        Path(file_path).resolve()
        if file_path
        else Path(__file__).resolve().parent.parent / "data" / "review_keywords.csv"
    )

    if not csv_path.exists():
        logger.error(f"review_keywords.csv not found at {csv_path}")
        raise FileNotFoundError(f"No CSV at {csv_path}")

    df = pd.read_csv(csv_path)

    # Validate columns
    expected = {"category", "keywords"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns {missing}; found {list(df.columns)}")

    # Split comma-separated keywords, trim blanks
    df["keywords"] = (
        df["keywords"]
        .astype(str)
        .apply(lambda s: [kw.strip() for kw in s.split(",") if kw.strip()])
    )

    categories = dict(zip(df["category"], df["keywords"]))
    logger.info(f"Loaded {len(categories)} categories from {csv_path}")
    return categories

