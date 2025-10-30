import json
import logging

from flask import Request  # <- optional; remove if not using type hints
from app.utils import last_complete_fri_to_thu, get_reviews
from app.summarizer import generate_summaries, load_summaries
from app.sentiment_grader import generate_sentiment_grade, load_sentiment_grades

# ---- Configuration ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def summarize_and_load(request: Request):
    """
    Cloud Function entrypoint:
      - Gets last complete Fri–Thu range
      - Fetches reviews
      - Generates wins/opps summaries + sentiment grades
      - Inserts them into BigQuery
    """
    try:
        start_date, end_date = last_complete_fri_to_thu()
        logger.info("Processing reviews from %s to %s", start_date, end_date)

        reviews = get_reviews(start_date, end_date)
        review_length = len(reviews)
        logger.info("Fetched %d reviews", review_length)

        # Summarizer
        wins, opps = generate_summaries(reviews)

        # Sentiment grader
        logger.info("Running sentiment grader for the same review batch...")
        graded_data = generate_sentiment_grade(reviews, output_response=True)
        sentiment_ok = load_sentiment_grades(start_date, end_date, graded_data)
        if sentiment_ok:
            logger.info("Sentiment grades inserted successfully.")
        else:
            logger.warning("Sentiment grades failed to insert.")

        # Store summaries
        summary_ok = load_summaries(start_date, end_date, wins, opps, review_length)
        if summary_ok:
            logger.info("Summaries inserted successfully.")
        else:
            logger.warning("Summaries failed to insert.")

        body = {
            "date_range": f"{start_date} to {end_date}",
            "review_count": review_length,
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

if __name__ == "__main__":
    # Local test run (no HTTP)
    start_date, end_date = last_complete_fri_to_thu()
    print(f"Processing week: {start_date} → {end_date}")

    reviews = get_reviews(start_date, end_date)
    review_length = len(reviews)
    print(f"Fetched {review_length} reviews")

    # Summarizer
    print("\nRunning summarizer...")
    wins, opps = generate_summaries(reviews)
    summary_status = load_summaries(start_date, end_date, wins, opps, review_length)
    print(f"Summary load status: {summary_status}")

    # Sentiment grader
    print("\nRunning sentiment grader...")
    graded_data = generate_sentiment_grade(reviews, output_response=True)
    sentiment_status = load_sentiment_grades(start_date, end_date, graded_data)
    print(f"Sentiment load status: {sentiment_status}")

    print("\n✅ Completed summarizer + sentiment grader flow.")
