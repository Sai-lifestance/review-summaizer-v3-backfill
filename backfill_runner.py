# backfill_runner.py
# One-time batch runner to backfill review summaries + sentiment grades + tags
# for a large date range (ex: 1 year) in Fri→Thu weekly windows.

import os
import logging
from datetime import date, datetime, timedelta
from typing import List

import pandas as pd

from app.clients import bq_client
from app.config import (
    BQ_PROJECT_SUMMARIES,
    SUMMARY_TABLE,
    SENTIMENT_GRADE_TABLE,
    REVIEW_TAGS_TABLE,
)
from app.utils import get_reviews
from app.summarizer import generate_summaries, load_summaries
from app.sentiment_grader import generate_sentiment_grade, load_sentiment_grades
from app.review_tagger import tag_and_load_review_tags

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill")


def _to_date(v) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return date.fromisoformat(v[:10])
    raise ValueError(f"Could not parse date from: {v}")


def _fq(project: str, table: str) -> str:
    # table is like "ai_generated_outputs.review_summaries"
    return f"`{project}.{table}`"


def _delete_week_window(start_d: date, end_d: date, versions: List[str]) -> None:
    """
    Delete existing rows for this week so reruns don't create duplicates.
    """
    ws = start_d.isoformat()
    we = end_d.isoformat()

    summary_fq = _fq(BQ_PROJECT_SUMMARIES, SUMMARY_TABLE)
    sentiment_fq = _fq(BQ_PROJECT_SUMMARIES, SENTIMENT_GRADE_TABLE)
    tags_fq = _fq(BQ_PROJECT_SUMMARIES, REVIEW_TAGS_TABLE)

    logger.info("Deleting existing rows for %s → %s", ws, we)

    # Summaries (no mapping_version)
    bq_client.query(
        f"DELETE FROM {summary_fq} WHERE week_start = '{ws}' AND week_end = '{we}'"
    ).result()

    # Sentiment + Tags (by mapping_version)
    for v in versions:
        bq_client.query(
            f"""
            DELETE FROM {sentiment_fq}
            WHERE week_start = '{ws}' AND week_end = '{we}'
              AND mapping_version = '{v}'
            """
        ).result()

        bq_client.query(
            f"""
            DELETE FROM {tags_fq}
            WHERE week_start = '{ws}' AND week_end = '{we}'
              AND mapping_version = '{v}'
            """
        ).result()


def _iter_fri_thu_weeks(start_d: date, end_d: date):
    """
    Generate Fri→Thu windows covering the range.
    IMPORTANT: Pass a Friday as start_d and a Thursday as end_d for clean weeks.
    """
    cur = start_d
    while cur <= end_d:
        yield cur, cur + timedelta(days=6)
        cur += timedelta(days=7)


def main():
    # Control the backfill via ENV VARS (easy to set in Cloud Run Job)
    # Example:
    #   BACKFILL_START=2025-01-03  (Friday)
    #   BACKFILL_END=2026-01-01    (Thursday)
    start_str = os.getenv("BACKFILL_START")
    end_str = os.getenv("BACKFILL_END")
    if not start_str or not end_str:
        raise RuntimeError("Set BACKFILL_START and BACKFILL_END env vars (YYYY-MM-DD).")

    start_d = _to_date(start_str)
    end_d = _to_date(end_str)

    # Whether to delete existing rows for each week before loading (prevents duplicates)
    do_delete = os.getenv("BACKFILL_DELETE", "true").lower() in ("1", "true", "yes", "y")

    # Which mapping versions to run. Default matches production: v1 + v2 + v3
    versions = os.getenv("BACKFILL_VERSIONS", "v1.0,v2.0,v3.0").split(",")
    versions = [v.strip() for v in versions if v.strip()]

    # Map version → keywords CSV in repo /data
    version_to_csv = {
        "v1.0": "data/review_keywords_v1.csv",
        "v2.0": "data/review_keywords_v2.csv",
        "v3.0": "data/review_keywords_v3.csv",
    }

    logger.info("Backfill range: %s → %s", start_d, end_d)
    logger.info("Delete before load: %s", do_delete)
    logger.info("Versions: %s", versions)

    for v in versions:
        if v not in version_to_csv:
            raise RuntimeError(f"Unknown version '{v}'. Allowed: {list(version_to_csv.keys())}")

    for ws, we in _iter_fri_thu_weeks(start_d, end_d):
        logger.info("====================================================")
        logger.info("WEEK: %s → %s", ws, we)

        # 1) Pull reviews from BQ
        reviews = get_reviews(ws, we)
        review_count = len(reviews)
        logger.info("Fetched %d reviews", review_count)

        # Backfill: if a whole week has 0 reviews, skip (saves LLM cost)
        if review_count == 0:
            logger.warning("No reviews for this week. Skipping.")
            continue

        reviews_df = pd.DataFrame(reviews)

        # 2) Delete existing outputs for that week (prevents duplicates)
        if do_delete:
            _delete_week_window(ws, we, versions)

        # 3) Summaries (run once per week)
        logger.info("Running summarizer...")
        wins, opps = generate_summaries(reviews)
        ok = load_summaries(ws.isoformat(), we.isoformat(), wins, opps, review_count)
        logger.info("Summary load: %s", "OK" if ok else "FAILED")

        # 4) Tagging + Sentiment per version
        for v in versions:
            csv_path = version_to_csv[v]

            logger.info("Running tagger (%s) using %s", v, csv_path)
            tagged_rows = tag_and_load_review_tags(
                reviews_df,
                ws.isoformat(),
                we.isoformat(),
                keywords_file=csv_path,
                mapping_version=v,
            )
            logger.info("Tagged rows loaded (%s): %s", v, tagged_rows)

            logger.info("Running sentiment grader (%s)...", v)
            graded = generate_sentiment_grade(
                reviews,
                output_response=True,
                keywords_file=csv_path,
            )
            sent_ok = load_sentiment_grades(ws.isoformat(), we.isoformat(), graded, mapping_version=v)
            logger.info("Sentiment load (%s): %s", v, "OK" if sent_ok else "FAILED")

    logger.info("✅ Backfill completed.")


if __name__ == "__main__":
    main()
