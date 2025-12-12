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
    keywords_file: str | None = None,
    mapping_version: str | None = None,
) -> pd.DataFrame:
    """
    Tag each review with zero or more categories based on keyword matches.
    Accepts:
      - pandas DataFrame
      - list of dicts (converted to DF internally)

    Only text_key + date_col are required.
    If pk_key is missing, DataFrame index will be used as foreign key.
    """

    # Convert list-of-dicts to DataFrame
    if isinstance(reviews_df, list):
        reviews_df = pd.DataFrame(reviews_df)

    if categories is None:
        categories = load_review_categories(keywords_file)

    # Require text + date only
    required_cols = {text_key, date_col}
    missing = required_cols - set(reviews_df.columns)
    if missing:
        raise ValueError(f"reviews_df is missing required columns: {missing}")

    # Primary key optional
    has_pk = pk_key in reviews_df.columns
    if not has_pk:
        logger.warning(
            "Column '%s' not found in reviews_df; using DataFrame index as review_foreign_key.",
            pk_key,
        )

    matches: list[dict] = []

    # Iterate reviews
    for idx, row in reviews_df.iterrows():
        review_raw = row[text_key]
        review_text = str(review_raw).lower()
        review_date = row[date_col]

        # Use provided PK or fallback index
        primary_key = row[pk_key] if has_pk else idx

        # Loop categories + keywords
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
        logger.info("No keyword matches found; returning empty DataFrame.")
        return tagged_df

    # Remove duplicates
    tagged_df = tagged_df.drop_duplicates(
        subset=["review_foreign_key", "category", "keyword"]
    )

    logger.info(
        "Tagged %d reviews into %d rows.",
        len(reviews_df),
        len(tagged_df),
    )
    tagged_df["mapping_version"] = mapping_version

    return tagged_df


def load_review_tags_to_bq(
    tagged_df: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> int:
    """
    Load tagged rows into BigQuery ai_generated_outputs.review_category_tags.
    Returns number of rows loaded.
    """
    if tagged_df.empty:
        logger.info("Tagged DataFrame is empty; skipping BigQuery load.")
        return 0

    tagged_df = tagged_df.copy()
    tagged_df["week_start"] = start_date
    tagged_df["week_end"] = end_date
    tagged_df["_load_ts_utc"] = datetime.now(timezone.utc)

    table_id = f"{BQ_PROJECT_SUMMARIES}.{REVIEW_TAGS_TABLE}"
    logger.info("Loading %d tagged rows into %s ...", len(tagged_df), table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = bq_client.load_table_from_dataframe(tagged_df, table_id, job_config=job_config)
    job.result()

    logger.info("Finished loading tagged rows into %s.", table_id)
    return len(tagged_df)


def tag_and_load_review_tags(
    reviews_df,
    start_date: str,
    end_date: str,
    keywords_file: str,
    mapping_version: str,
) -> int:
    """
    Helper wrapper called from main.py
    """
    tagged_df = tag_reviews_dataframe(
        reviews_df=reviews_df,
        keywords_file=keywords_file,
        mapping_version=mapping_version,
    )
    return load_review_tags_to_bq(tagged_df, start_date, end_date)

