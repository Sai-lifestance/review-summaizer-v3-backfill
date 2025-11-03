import datetime
import json
import logging
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import pandas as pd

from app.clients import openai_client, bq_client
from app.config import BQ_PROJECT_SUMMARIES, DEFAULT_MODEL, SENTIMENT_GRADE_TABLE
from app.utils import load_review_categories, get_reviews

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _bq_json_sanitize(x):
    """
    Recursively convert Python types to JSON-serializable forms acceptable
    for BigQuery insert_rows_json.
    - date/datetime -> ISO string
    - Decimal -> float (or str if you prefer STRING in BQ)
    """
    if isinstance(x, date) and not isinstance(x, datetime):
        return x.isoformat()  # 'YYYY-MM-DD'
    if isinstance(x, datetime):
        return x.isoformat()  # 'YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]'
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {k: _bq_json_sanitize(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_bq_json_sanitize(v) for v in x]
    return x
def count_category_mentions(reviews, categories, text_key="review_comment"):
    """
    Counts how many reviews mention each category at least once based on its keyword list.
    reviews: list[dict|str]
    categories: dict[str, list[str]]
    returns: dict[str, int]
    """
    categories = categories or {}
    reviews = reviews or []

    logger.info("[sentiment] start: reviews=%s categories=%s", len(reviews), len(categories))

    total_kw = sum(len(v or []) for v in categories.values())
    empty_kw = sum(1 for v in categories.values() for k in (v or []) if not k)
    if empty_kw:
        logger.info(
            "[sentiment] keywords: total=%d empty=%d (first 5 affected categories: %s)",
            total_kw,
            empty_kw,
            [c for c, v in categories.items() if any(k in (None, "") for k in (v or []))][:5],
        )

    counts = defaultdict(int)
    skipped = 0
    bad_samples = []

    for i, review in enumerate(reviews):
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
            for kw in (k for k in kws if k):  # skip None/empty
                kw_l = str(kw).lower()
                if kw_l and kw_l in text:
                    counts[cat] += 1
                    break  # count once per category

    if skipped:
        logger.info("[sentiment] skipped_reviews=%d (first few: %s)", skipped, bad_samples)

    logger.info("[sentiment] done: matched_categories=%d", len(counts))
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
        logger.info(
            "[sentiment] loaded categories=%d total_keywords=%d empty_keywords=%d keys_sample=%s",
            len(categories),
            total_kw,
            empty_kw,
            list(categories.keys())[:10],
        )
    except Exception as e:
        logger.exception("[sentiment] failed to load categories: %s", e)
        return [{"error": f"Failed loading categories: {e}"}]

    # ── count mentions (safe) ──────────────────────────────────
    try:
        logger.info("Calling count_category_mentions...")
        mention_counts = count_category_mentions(reviews, categories, text_key="review_comment")
        logger.info(
            "count_category_mentions completed. Categories matched: %s",
            list(mention_counts.keys())[:10],
        )
    except Exception as e:
        logger.exception("[sentiment] count_category_mentions crashed: %s", e)
        return [{"error": f"Counter failed: {e}"}]

    # ── build prompt safely ────────────────────────────────────
    def _safe_str(v):
        if v is None:
            return ""
        return v if isinstance(v, str) else str(v)

    LINE_TRIM = 10000  # trim overly long individual reviews
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

    if skipped:
        logger.info(
            "[sentiment] skipped %d reviews with empty text while building prompt", skipped
        )

    # ✅ this line must be indented inside the function
    logger.info(
        "[sentiment] prompt will include all %d reviews (trim=%d chars per line)",
        len(review_texts_list),
        LINE_TRIM,
    )

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

        mention_lower = {str(k).lower(): v for k, v in (mention_counts or {}).items()}
        for entry in graded_data:
            cat = _safe_str(entry.get("category"))
            entry["mentions"] = mention_lower.get(cat.lower(), 0)

        logger.info("Merged mention counts with AI output. rows=%d", len(graded_data))
    except Exception as e:
        logger.warning("Error merging mentions or parsing model output: %s", e)
        return [{"raw_response": content, "mention_counts": mention_counts}]

    # ── optional file output ───────────────────────────────────
    if output_response:
        try:
            import os

            os.makedirs("tmp", exist_ok=True)
            with open("tmp/graded_sentiment.txt", "w", encoding="utf-8") as f:
                f.write(json.dumps(graded_data, indent=2))
            logger.info("Successfully wrote graded sentiment to tmp/graded_sentiment.txt")
        except Exception as e:
            logger.warning("Failed writing graded sentiment file: %s", e)

    logger.info("[sentiment] leave generate_sentiment_grade: rows=%d", len(graded_data))
    return graded_data


def load_sentiment_grades(start_date, end_date, graded_data):
    table_id = f"{BQ_PROJECT_SUMMARIES}.{SENTIMENT_GRADE_TABLE}"
    logger.info("Loading sentiment grades into %s...", table_id)

    rows_to_insert = []
    iso_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for entry in graded_data or []:
        category = entry.get("category")
        if not category:
            continue
        rows_to_insert.append(
            {
                "week_start": start_date,
                "week_end": end_date,
                "review_category": category,
                "sentiment_grade": entry.get("grade"),
                "count_of_mentions": entry.get("mentions", 0),
                "insert_timestamp_utc": iso_now,
            }
        )

    if not rows_to_insert:
        logger.warning("No valid rows to insert into BigQuery table %s", table_id)
        return False

    rows_safe = _bq_json_sanitize(rows_to_insert)
    errors = bq_client.insert_rows_json(table_id, rows_safe)
    if errors:
        logger.error("Error inserting summary: %s", errors)
        return False

    logger.info("Sentiment grades inserted successfully.")
    return True


if __name__ == "__main__":
    start_date = "2025-09-03"
    end_date = "2025-09-09"
    reviews = get_reviews(start_date, end_date)
    print(f"Count of reviews: {len(reviews)}")
    graded_data = generate_sentiment_grade(reviews, output_response=True)
    print(graded_data)
    load_status = load_sentiment_grades(start_date, end_date, graded_data)
    print(load_status)

