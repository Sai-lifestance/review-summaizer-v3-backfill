import logging
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

from app.clients import bq_client
from app.config import BQ_PROJECT_SUMMARIES, REVIEW_TAGS_TABLE
from app.utils import load_review_categories

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def tag_reviews_dataframe(
    reviews_df: pd.DataFrame,
    categories: dict | None = None,
    text_key: str = "review_comment",
    pk_key: str = "primary_key",
    date_col: str = "date",
) -> pd.DataFrame:
    """
    Tag each review with zero or more categories based on simple keyword matches.

    Returns a long-format DataFrame with one row per (review, category, keyword).
    """
    if categories is None:
        categories = load_review_categories()

    required_cols = {text_key, pk_key, date_col}
    missing = required_cols - set(reviews_df.columns)
    if missing:
        raise ValueError(f"reviews_df is missing required columns: {missing}")

    matches: list[dict] = []

    for _, row in reviews_df.iterrows():
        review_raw = row[text_key]
        review_text = str(review_raw).lower()
        primary_key = row[pk_key]
        review_date = row[date_col]

        for category, keywords in categories.items():
            for kw in keywords:
                kw_norm = kw.strip().lower()
                if not kw_norm:
                    continue

                if kw_norm in review_text:
                    matches.append(
                        {
                            "review_foreign_key": primary_key,
                            "category": category,
                            "keyword": kw_norm,
                            "review_comment": review_raw,
                            "review_date": review_date,
                        }
                    )

    tagged_df = pd.DataFrame(matches)
    if tagged_df.empty:
        logger.info("No keyword matches found; returning empty tagged DataFrame.")
        return tagged_df

    # Avoid duplicate (review, category, keyword) rows
    tagged_df = tagged_df.drop_duplicates(
        subset=["review_foreign_key", "category", "keyword"]
    )

    logger.info(
        "Tagged %d reviews into %d (review, category, keyword) rows.",
        len(reviews_df),
        len(tagged_df),
    )
    return tagged_df


def load_review_tags_to_bq(
    tagged_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> int:
    """
    Load tagged reviews into BigQuery ai_generated_outputs.review_category_tags.
    Returns number of rows loaded.
    """
    if tagged_df.empty:
        logger.info("Tagged DataFrame is empty; skipping BigQuery load.")
        return 0

    tagged_df = tagged_df.copy()
    tagged_df["week_start"] = start_date
    tagged_df["week_end"] = end_date
    tagged_df["load_ts_utc"] = datetime.now(timezone.utc)

    table_id = f"{BQ_PROJECT_SUMMARIES}.{REVIEW_TAGS_TABLE}"
    logger.info("Loading %d tagged rows into %s ...", len(tagged_df), table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = bq_client.load_table_from_dataframe(tagged_df, table_id, job_config=job_config)
    job.result()

    logger.info("Finished loading tagged reviews into %s.", table_id)
    return len(tagged_df)


def tag_and_load_review_tags(
    reviews_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> int:
    """
    Convenience wrapper used from main.py:
    - tags the reviews
    - loads the result into BigQuery
    """
    tagged_df = tag_reviews_dataframe(reviews_df)
    return load_review_tags_to_bq(tagged_df, start_date, end_date)
