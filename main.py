import json
import logging
from flask import Flask, request, jsonify
from app.utils import last_complete_fri_to_thu, get_reviews
from app.summarizer import generate_summaries, load_summaries


# ---- Configuration ----
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# @app.route("/summarize-and-load", methods=["POST"])
def summarize_and_load(request):
    """
    A Function that:
        - Gets last complete Fri–Thu range
        - Fetches reviews
        - Generates wins/opps summaries
        - Inserts them into BigQuery

    Returns:
        - A Dictionary of metadata:
            - date_range: date range of reviews summarized
            - review_count
            - wins_summary
            - opps_summary
            - status: "inserted" if succeeded, "failed" if error
        - Response Code
        - Content-Type
    """
    try:
        start_date, end_date = last_complete_fri_to_thu()
        logging.info("Processing reviews from %s to %s", start_date, end_date)

        reviews = get_reviews(start_date, end_date)
        review_length = len(reviews)
        logging.info("Fetched %d reviews", review_length)

        wins, opps = generate_summaries(reviews)
        success = load_summaries(start_date, end_date, wins, opps, review_length)

        if success:
            logging.info("Summaries inserted successfully.")
        else:
            logging.warning("Summaries failed to insert.")

        return (json.dumps({
            "date_range": f"{start_date} to {end_date}",
            "review_count": review_length,
            "wins_summary": wins,
            "opps_summary": opps,
            "status": "inserted" if success else "failed"
        }), 200, {"Content-Type": "application/json"})
    
    except Exception as e:
        logging.exception("Unhandled error: ")
        return json.dumps({
            "error": str(e),
            "status": "failed"
        }), 500
    


if __name__ == "__main__":

    start_date, end_date = last_complete_fri_to_thu()
    print(start_date)
    print(end_date)

    reviews = get_reviews("2025-09-01", "2025-09-01")
    review_length = len(reviews)

    print(review_length)
