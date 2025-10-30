import datetime
import pandas as pd
from app.clients import openai_client, bq_client
from app.config import BQ_PROJECT_SUMMARIES, DEFAULT_MODEL, SENTIMENT_GRADE_TABLE
from app.utils import load_review_categories, get_reviews
import logging
import re
from collections import defaultdict
import json
import logging
from collections import defaultdict

log = logging.getLogger("app.sentiment")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def count_category_mentions(reviews, category_keywords, text_key="review_comment"):
    """
    Counts how many reviews mention each category at least once based on its keyword list.

    Args:
        reviews (list): List of review texts or dicts (e.g., [{"review_comment": "..."}]).
        category_keywords (dict): Dict mapping category -> list of keywords.
        text_key (str): If reviews are dicts, which key contains the review text.

    Returns:
        dict: {category: mention_count}
    """
   def count_category_mentions(reviews, categories, text_key="review_comment"):
    log.info("[sentiment] start: reviews=%s categories=%s", len(reviews or []), len(categories or {}))

    total_kw = sum(len(v or []) for v in categories.values())
    empty_kw = sum(1 for v in categories.values() for k in (v or []) if not k)
    if empty_kw:
        log.info("[sentiment] keywords: total=%d empty=%d (first 5 affected categories: %s)",
                 total_kw, empty_kw,
                 [c for c, v in categories.items() if any(k in (None, "") for k in (v or []))][:5])

    counts = defaultdict(int)
    skipped = 0
    bad_samples = []

    for i, review in enumerate(reviews or []):
        text = review.get(text_key) if isinstance(review, dict) else review
        if not text:
            skipped += 1
            if len(bad_samples) < 5:
                bad_samples.append({"idx": i, "type": type(review).__name__, "text": None})
            continue

        text = str(text).lower()

        for cat, kws in categories.items():
            if not kws:
                continue
            for kw in (k for k in kws if k):
                kw_l = str(kw).lower()
                if kw_l and kw_l in text:
                    counts[cat] += 1
                    break  # count once per category

    if skipped:
        log.info("[sentiment] skipped_reviews=%d (first few: %s)", skipped, bad_samples)

    log.info("[sentiment] done: matched_categories=%d", len(counts))
    return dict(counts)


def generate_sentiment_grade(reviews,model=DEFAULT_MODEL,output_response=False):
    if not reviews:
        logger.warning("No reviews found to grade sentiment.")
        return [{"error": "No new reviews."}]
    
    # Load categories
    categories = load_review_categories()

    # Count mentions
    logger.info("Calling count_category_mentions...")
    mention_counts = count_category_mentions(reviews, categories)
    logger.info("count_category_mentions completed. Categories matched: %s", list(mention_counts.keys())[:10])
    
    # Combine all reviews for prompt
    review_texts = "\n".join([f"- {r['review_comment']}" for r in reviews if r['review_comment']])

    # Format categories for prompt
    categories_text = "\n".join(
        [f"- {cat}: {', '.join(kw_list)}" for cat, kw_list in categories.items()]
    )
    
    prompt = f"""
    You are a sentiment analysis expert. Analyze the following Google reviews and assign each review category a letter grade (A-F) (Plus and Minuses can be added too.) based on sentiment.

    Review categories and their keywords:
    {categories_text}

    Reviews:
    {review_texts}

    Respond in valid JSON format like this:
    [
        {{"category": "Billing", "grade": "A"}},
        {{"category": "Clinical Care and Outcomes", "grade": "B-"}}
    ]

    Grading scale:
    A = overwhelmingly positive
    B = mostly positive
    C = neutral/mixed
    D = mostly negative
    F = overwhelmingly negative
    """

    # Get response from AI
    content = None
    try:
        logger.info("Sending prompt to OpenAI...")
        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a structured sentiment analysis assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        logger.info("Waiting for response from OpenAI...")

        content = response.choices[0].message.content.strip()
        logger.info("Model response received for sentiment grading.")

    except Exception as e:
        logger.exception("Error generating sentiment grades via OpenAI")
        return [{"error": str(e)}]
    
    # Merge counts with AI output
    try:
        graded_data = json.loads(content)
        logger.info("Merging mention counts with AI output...")
        for entry in graded_data:
            cat = entry["category"]
            entry["mentions"] = mention_counts.get(cat, 0)
    except Exception as e:
        logger.warning(f"Error merging mentions or parsing model output: {e}")
        return [{"raw_response": content, "mention_counts": mention_counts}]
    
    if output_response:
        try:
            file_path = "tmp/graded_sentiment.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(graded_data, indent=2))
            logger.info(f"Successfully wrote to {file_path}")
        except Exception as e:
            print("Error occurred:", e)

    return graded_data

def load_sentiment_grades(start_date, end_date, graded_data):
    table_id = f"{BQ_PROJECT_SUMMARIES}.{SENTIMENT_GRADE_TABLE}"

    logger.info(f"Loading sentiment grades into {table_id}...")

    rows_to_insert = []
    iso_now = datetime.datetime.now().isoformat()
    for entry in graded_data:
        if "category" not in entry:
            continue

        rows_to_insert.append({
            "week_start": start_date,
            "week_end": end_date,
            "review_category": entry["category"],
            "sentiment_grade": entry.get("grade", None), 
            "count_of_mentions": entry.get("mentions", 0),
            "insert_timestamp_utc": iso_now
        })

    if not rows_to_insert:
        logger.warning(f"No valid rows to insert into BigQuery table {table_id}")
        return False

    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Error inserting summary: ", errors)
        return False
    else:
        print("Sentiment grades inserted successfully.")
        return True

if __name__ == "__main__":
    start_date = "2025-09-03"
    end_date = "2025-09-09"
    reviews = get_reviews(start_date, end_date)
    print(f"Count of reviews: {len(reviews)}")
    graded_data = generate_sentiment_grade(reviews,output_response=True)
    print(graded_data)
    load_status = load_sentiment_grades(start_date, end_date, graded_data)

    print(load_status)



