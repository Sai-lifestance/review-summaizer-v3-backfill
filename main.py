import json
import logging
from flask import Request
from datetime import date, datetime, timedelta
from collections import Counter

from app.utils import last_complete_fri_to_thu, get_reviews
from app.summarizer import generate_summaries, load_summaries
from app.sentiment_grader import generate_sentiment_grade, load_sentiment_grades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
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


# ──────────────────────────────────────────────────────────────────────────────
# Cloud Function entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def summarize_and_load(request: Request):
    """
    Cloud Function entrypoint:
      - Gets last complete Fri–Thu range
      - Fetches reviews
      - Validates last-week per-day completeness (abort if any day is zero)
      - Generates wins/opps summaries + sentiment grades
      - Inserts them into BigQuery
    """
    try:
        # 1) Window selection
        #start_date, end_date = last_complete_fri_to_thu()
        start_date = _to_date("2025-10-31")
        end_date   = _to_date("2025-11-06")
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

        # 3) 7-day completeness check (abort if any day has zero)
        all_days = [start_date + timedelta(days=i)
                    for i in range((end_date - start_date).days + 1)]
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
            logger.warning("Per-day count: skipped %d row(s) with missing/unparseable date", skipped)

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

        # 4) Summarizer
        logger.info("Running summarizer for %d reviews...", review_length)
        wins, opps = generate_summaries(reviews)

        # 5) Sentiment grader
        logger.info("Running sentiment grader for the same review batch...")
        graded_data = generate_sentiment_grade(reviews, output_response=True)
        sentiment_ok = load_sentiment_grades(start_date, end_date, graded_data)
        if sentiment_ok:
            logger.info("Sentiment grades inserted successfully.")
        else:
            logger.warning("Sentiment grades failed to insert.")

        # 6) Store summaries
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
            "sentiment_status": "inserted" if sentiment_ok else "failed",
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
    # start_date, end_date = last_complete_fri_to_thu()
    start_date, end_date = '2025-10-31', '2025-11-06'
    start_date = _to_date(start_date)
    end_date = _to_date(end_date)
    print(f"Processing week: {start_date} → {end_date}")

    reviews = get_reviews(start_date, end_date)
    review_length = len(reviews)
    print(f"Fetched {review_length} reviews")

    # # Validation for local run
    # all_days = [start_date + timedelta(days=i)
    #             for i in range((end_date - start_date).days + 1)]
    # cnt = Counter()
    # for r in reviews:
    #     d = _to_date(r.get("date"))
    #     if d and (start_date <= d <= end_date):
    #         cnt[d] += 1

    # print("──── Daily review counts ────")
    # zero_days = []
    # total = 0
    # for day in all_days:
    #     c = cnt.get(day, 0)
    #     total += c
    #     print(f"  {day.isoformat()} | {c:4d}")
    #     if c == 0:
    #         zero_days.append(day)
    # print(f"  Total (window): {total}")
    # print("─────────────────────────────")

    # if zero_days:
    #     raise RuntimeError(f"Reviews data incomplete: zero rows on {', '.join(d.isoformat() for d in zero_days)}")

    # Summarizer
    print("\nRunning summarizer...")
    wins, opps = generate_summaries(reviews)
    print("Successfully received responses:")
    print("---------------------------")
    print(wins)
    print(opps)
    
    print("Now Loading Summaries to Database....")
    summary_status = load_summaries(start_date, end_date, wins, opps, review_length)
    print(f"Summary load status: {summary_status}")
    

    # # Sentiment grader
    # print("\nRunning sentiment grader...")
    # graded_data = generate_sentiment_grade(reviews, output_response=True)
    # sentiment_status = load_sentiment_grades(start_date, end_date, graded_data)
    # print(f"Sentiment load status: {sentiment_status}")

    # print("\n✅ Completed summarizer + sentiment grader flow.")

