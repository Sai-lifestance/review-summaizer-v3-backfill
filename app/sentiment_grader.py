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

def count_category_mentions(reviews, categories, text_key="review_comment"):
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


def generate_sentiment_grade(reviews, model=DEFAULT_MODEL, output_response=False):
    # ── early guards ───────────────────────────────────────────
    total_reviews = len(reviews or [])
    logger.info("[sentiment] enter generate_sentiment_grade: reviews=%d", total_reviews)
    if not reviews:
        logger.warning("No reviews found to grade sentiment.")
        return [{"error": "No new reviews."}]

    # ── load categories ────────────────────────────────────────
    try:
        categories = load_review_categories()  # expects dict[str, list[str]]
        total_kw = sum(len(v or []) for v in categories.values())
        empty_kw = sum(1 for v in categories.values() for k in (v or []) if not k)
        logger.info("[sentiment] loaded categories=%d total_keywords=%d empty_keywords=%d keys_sample=%s",
                    len(categories), total_kw, empty_kw, list(categories.keys())[:10])
    except Exception as e:
        logger.exception("[sentiment] failed to load categories: %s", e)
        return [{"error": f"Failed loading categories: {e}"}]

    # ── count mentions (safe) ──────────────────────────────────
    try:
        logger.info("Calling count_category_mentions...")
        mention_counts = count_category_mentions(reviews, categories, text_key="review_comment")
        logger.info("count_category_mentions completed. Categories matched: %s",
                    list(mention_counts.keys())[:10])
    except Exception as e:
        logger.exception("[sentiment] count_category_mentions crashed: %s", e)
        return [{"error": f"Counter failed: {e}"}]

    # ── build prompt safely ────────────────────────────────────
    def _safe_str(v):
        if v is None:
            return ""
        return v if isinstance(v, str) else str(v)

    # extract review text safely, skip empties, trim each line to keep tokens sane
    LINES_LIMIT = 300          # cap lines included in the prompt
    LINE_TRIM = 500            # trim very long reviews
    review_texts_list = []
    skipped = 0
    for r in reviews:
        txt = r.get("review_comment") if isinstance(r, dict) else r
        if not txt:
            skipped += 1
            continue
        s = _safe_str(txt)
        if len(s) > LINE_TRIM:
            s = s[:LINE_TRIM] + "...[truncated]"
        review_texts_list.append(f"- {s}")
        if len(review_texts_list) >= LINES_LIMIT:
            break
    if skipped:
        logger.info("[sentiment] skipped %d reviews with empty text while building prompt", skipped)
    logger.info("[sentiment] prompt will include %d/%d reviews (limit=%d, trim=%d chars)",
                len(review_texts_list), total_reviews, LINES_LIMIT, LINE_TRIM)

    review_texts = "\n".join(review_texts_list)

    # format categories (skip empty keywords)
    categories_text = "\n".join(
        f"- {cat}: {', '.join([_safe_str(k) for k in (kw_list or []) if k])}"
        for cat, kw_list in categories.items()
    )

    prompt = f"""
You are a sentiment analysis expert. Analyze the following Google reviews and assign each review category a letter grade (A–F). Plus/minus grades are allowed.

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
""".strip()

    # ── call OpenAI ────────────────────────────────────────────
    content = None
    try:
        logger.info("Sending prompt to OpenAI... (model=%s, chars=%d)", model, len(prompt))
        resp = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a structured sentiment analysis assistant."},
                {"role": "user", "content": prompt},
            ],
        )
        content = (resp.choices[0].message.content or "").strip()
        logger.info("Model response received for sentiment grading. len=%d", len(content))
    except Exception as e:
        logger.exception("Error generating sentiment grades via OpenAI")
        return [{"error": f"OpenAI call failed: {e}"}]

    # ── merge counts with AI output ────────────────────────────
    try:
        graded_data = json.loads(content)
        if not isinstance(graded_data, list):
            raise ValueError("Model JSON is not a list")

        # make category lookup case-insensitive
        mention_lower = {str(k).lower(): v for k, v in (mention_counts or {}).items()}
        for entry in graded_data:
            cat = _safe_str(entry.get("category"))
            entry["mentions"] = mention_lower.get(cat.lower(), 0)

        logger.info("Merged mention counts with AI output. rows=%d", len(graded_data))
    except Exception as e:
        logger.warning("Error merging mentions or parsing model output: %s", e)
        # Return raw content and counts so you can inspect in logs/UI
        return [{"raw_response": content, "mention_counts": mention_counts}]

    # ── optional file output ───────────────────────────────────
    if output_response:
        try:
            file_path = "tmp/graded_sentiment.txt"
            import os
            os.makedirs("tmp", exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(graded_data, indent=2))
            logger.info("Successfully wrote graded sentiment to %s", file_path)
        except Exception as e:
            logger.warning("Failed writing graded sentiment file: %s", e)

    logger.info("[sentiment] leave generate_sentiment_grade: rows=%d", len(graded_data))
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





