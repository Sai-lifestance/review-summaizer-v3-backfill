import json
import logging
import os
from flask import Request
from datetime import date, datetime, timedelta
from collections import Counter
import pandas as pd

from app.utils import last_complete_fri_to_thu, get_reviews
from app.summarizer import generate_summaries, load_summaries
from app.sentiment_grader import generate_sentiment_grade, load_sentiment_grades
from app.review_tagger import tag_and_load_review_tags

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("app")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _to_date(v):
    """Coerce common types/strings to a Python date (YYYY-MM-DD)."""
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        # supports 'YYYY-MM-DD' and ISO-like strings
        return date.fromisoformat(v[:10])
    return None


def _get_override_window(request: Request):
    """
    Optional override window for backfill.

    Supports:
      - Query params: ?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
      - JSON body: {"start_date":"YYYY-MM-DD","end_date":"YYYY-MM-DD"}
      - Env vars: START_DATE/END_DATE or BACKFILL_START/BACKFILL_END

    Returns: (start_date: date|None, end_date: date|None)
    """
    # 1) request query params
    try:
        if request is not None and getattr(request, "args", None):
            s = request.args.get("start_date")
            e = request.args.get("end_date")
            if s and e:
                return _to_date(s), _to_date(e)
    except Exception:
        pass

    # 2) request JSON body
    try:
        if request is not None:
            payload = request.get_json(silent=True) or {}
            s = payload.get("start_date")
            e = payload.get("end_date")
            if s and e:
                return _to_date(s), _to_date(e)
    except Exception:
        pass

    # 3) env vars (good for Cloud Run Jobs / local runs)
    s = os.getenv("START_DATE") or os.getenv("BACKFILL_START")
    e = os.getenv("END_DATE") or os.getenv("BACKFILL_END")
    if s and e:
        return _to_date(s), _to_date(e)

    return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Cloud Function entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def summarize_and_load(request: Request):
    """
    Cloud Function entrypoint:
      - Gets last complete Fri–Thu range (unless overridden by backfill dates)
      - Fetches reviews
      - Validates last-week per-day completeness (abort if any day is zero)
      - Runs summarizer ONCE
      - Runs tagger + sentiment for each mapping version (v1, v2, ...)
      - Inserts results into BigQuery
    """
    try:
        # 1) Window selection (default) OR override for backfill
        start_date, end_date = _get_override_window(request)
        if not (start_date and end_date):
            start_date, end_date = last_complete_fri_to_thu()

        # Ensure real date objects (avoid str - str TypeError)
        start_date = _to_date(start_date)
        end_date = _to_date(end_date)
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            raise RuntimeError("Could not coerce start/end window to dates")

        logger.info("Processing reviews from %s to %s", start_date, end_date)

        # 2) Pull reviews for the window
        reviews = get_reviews(start_date, end_date)
        review_length = len(reviews)
        logger.info("Fetched %d reviews", review_length)

        # Convert into dataframe (used by tagger)
        reviews_df = pd.DataFrame(reviews)

        # Keyword mapping versions (add more if needed)
        VERSIONS = [
            ("v1.0", "data/review_keywords_v1.csv"),
            ("v2.0", "data/review_keywords_v2.csv"),
            ("v3.0", "data/review_keywords_v3.csv"),
        ]

        # 3) 7-day completeness check (abort if any day has zero)
        all_days = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]
        cnt = Counter()
        skipped = 0

        for r in reviews:
            d = _to_date(r.get("date"))  # your rows have a 'date' field
            if d is None:
                skipped += 1
                continue
            if start_date <= d <= end_date:
                cnt[d] += 1

        if skipped:
            logger.warning(
                "Per-day count: skipped %d row(s) with missing/unparseable date",
                skipped,
            )

        logger.info("──── Daily review counts %s → %s ────", start_date, end_date)
        zero_days = []
        total = 0
        for day in all_days:
            c = cnt.get(day, 0)
            total += c
            logger.info("  %s | %4d", day.isoformat(), c)
            if c == 0:
                zero_days.append(day)
        logger.info("  Total (window): %d", total)
        logger.info("────────────────────────────────────────")

        if zero_days:
            z = ", ".join(day.isoformat() for day in zero_days)
            raise RuntimeError(f"Reviews data incomplete: zero rows on {z}. Aborting run.")

        # 4) Summarizer (RUN ONCE)
        logger.info("Running summarizer for %d reviews...", review_length)
        wins, opps = generate_summaries(reviews)

        # 5) Tagger + Sentiment grader (RUN PER VERSION)
        sentiment_results = {}
        tagger_results = {}

        for mapping_version, keywords_file in VERSIONS:
            logger.info(
                "Running tagger + sentiment for mapping_version=%s using %s",
                mapping_version,
                keywords_file,
            )

            # Tagger
            print(f"\nRunning review category tagger ({mapping_version})...")
            tagged_rows = tag_and_load_review_tags(
                reviews_df,
                start_date,
                end_date,
                keywords_file=keywords_file,
                mapping_version=mapping_version,
            )
            print(f"Tagged + loaded {tagged_rows} rows into BigQuery for {mapping_version}.")
            tagger_results[mapping_version] = tagged_rows

            # Sentiment grader
            logger.info("Running sentiment grader (%s)...", mapping_version)
            graded_data = generate_sentiment_grade(
                reviews,
                output_response=True,
                keywords_file=keywords_file,
            )

            sentiment_ok = load_sentiment_grades(
                start_date,
                end_date,
                graded_data,
                mapping_version=mapping_version,
            )

            sentiment_results[mapping_version] = "inserted" if sentiment_ok else "failed"
            if sentiment_ok:
                logger.info("Sentiment grades inserted successfully for %s.", mapping_version)
            else:
                logger.warning("Sentiment grades failed to insert for %s.", mapping_version)

        # 6) Store summaries (after we successfully generated them)
        logger.info("Loading summaries into BigQuery...")
        summary_ok = load_summaries(start_date, end_date, wins, opps, review_length)
        if summary_ok:
            logger.info("Summaries inserted successfully.")
        else:
            logger.warning("Summaries failed to insert.")

        body = {
            "date_range": f"{start_date} to {end_date}",
            "review_count": review_length,
            "daily_counts": {d.isoformat(): cnt.get(d, 0) for d in all_days},
            "wins_summary": wins,
            "opps_summary": opps,
            "summary_status": "inserted" if summary_ok else "failed",
            "tagger_rows_by_version": tagger_results,
            "sentiment_status_by_version": sentiment_results,
        }
        return json.dumps(body), 200, {"Content-Type": "application/json"}

    except Exception as e:
        logger.exception("Unhandled error")
        return json.dumps({"error": str(e), "status": "failed"}), 500, {
            "Content-Type": "application/json"
        }


# ──────────────────────────────────────────────────────────────────────────────
# Local test runner
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Local test run (no HTTP)
    # You can override with env vars:
    #   START_DATE=2025-10-31 END_DATE=2025-11-06 python main.py
    start_date, end_date = _get_override_window(request=None)
    if not (start_date and end_date):
        start_date, end_date = "2025-10-31", "2025-11-06"

    start_date = _to_date(start_date)
    end_date = _to_date(end_date)
    print(f"Processing week: {start_date} → {end_date}")

    reviews = get_reviews(start_date, end_date)
    review_length = len(reviews)
    print(f"Fetched {review_length} reviews")

    reviews_df = pd.DataFrame(reviews)

    VERSIONS = [
        ("v1.0", "data/review_keywords_v1.csv"),
        ("v2.0", "data/review_keywords_v2.csv"),
        ("v3.0", "data/review_keywords_v3.csv"),
    ]

    print("\nRunning summarizer...")
    wins, opps = generate_summaries(reviews)
    print("Successfully received responses:")
    print("---------------------------")
    print(wins)
    print(opps)

    print("Now Loading Summaries to Database....")
    summary_status = load_summaries(start_date, end_date, wins, opps, review_length)
    print(f"Summary load status: {summary_status}")

    for mapping_version, keywords_file in VERSIONS:
        print(f"\nRunning tagger ({mapping_version})...")
        tagged_rows = tag_and_load_review_tags(
            reviews_df,
            start_date,
            end_date,
            keywords_file=keywords_file,
            mapping_version=mapping_version,
        )
        print(f"Tagged rows loaded: {tagged_rows}")

        print(f"Running sentiment grader ({mapping_version})...")
        graded_data = generate_sentiment_grade(
            reviews,
            output_response=True,
            keywords_file=keywords_file,
        )
        sentiment_status = load_sentiment_grades(
            start_date,
            end_date,
            graded_data,
            mapping_version=mapping_version,
        )
        print(f"Sentiment load status ({mapping_version}): {sentiment_status}")

    print("\n✅ Completed summarizer + versioned tagger + versioned sentiment flow.")
