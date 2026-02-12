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


def _get_auto_loop_flag(request: Request) -> bool:
    """
    Enable looping inside a single request.
    Supports env var AUTO_LOOP=true and request JSON/query auto_loop=true
    """
    env_flag = (os.getenv("AUTO_LOOP", "false").lower() == "true")
    if request is None:
        return env_flag

    try:
        if getattr(request, "args", None):
            q = request.args.get("auto_loop")
            if q is not None:
                return str(q).lower() in ("1", "true", "yes", "y")
    except Exception:
        pass

    try:
        payload = request.get_json(silent=True) or {}
        if "auto_loop" in payload:
            return str(payload.get("auto_loop")).lower() in ("1", "true", "yes", "y")
    except Exception:
        pass

    return env_flag


def _align_to_friday(d: date) -> date:
    # Mon=0..Sun=6; Fri=4
    shift = (d.weekday() - 4) % 7
    return d - timedelta(days=shift)


def _align_to_thursday(d: date) -> date:
    # Thu=3
    shift = (3 - d.weekday()) % 7
    return d + timedelta(days=shift)


def _iter_fri_thu_weeks(start: date, end: date):
    """
    Yield (week_start, week_end) in 7-day chunks Fri->Thu, inclusive.
    Assumes start is Friday and end is Thursday (aligned already).
    """
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=6), end)
        cur = cur + timedelta(days=7)


# ──────────────────────────────────────────────────────────────────────────────
# Core “single window” run
# ──────────────────────────────────────────────────────────────────────────────
def process_window(start_date: date, end_date: date) -> dict:
    """
    Runs EXACTLY what your code used to do for one window.
    Returns a small dict summary for logging + HTTP response.
    """
    logger.info("Processing reviews from %s to %s", start_date, end_date)

    # 1) Pull reviews for the window
    reviews = get_reviews(start_date, end_date)
    review_length = len(reviews)
    logger.info("Fetched %d reviews", review_length)

    # Convert into dataframe (used by tagger)
    reviews_df = pd.DataFrame(reviews)

    # Keyword mapping versions
    VERSIONS = [
        ("v1.0", "data/review_keywords_v1.csv"),
        ("v2.0", "data/review_keywords_v2.csv"),
        ("v3.0", "data/review_keywords_v3.csv"),
    ]

    # 2) Completeness check (abort if any day is zero)
    all_days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    cnt = Counter()
    skipped = 0

    for r in reviews:
        d = _to_date(r.get("date"))
        if d is None:
            skipped += 1
            continue
        if start_date <= d <= end_date:
            cnt[d] += 1

    if skipped:
        logger.warning("Per-day count: skipped %d row(s) with missing/unparseable date", skipped)

    zero_days = [day for day in all_days if cnt.get(day, 0) == 0]
    if zero_days:
        z = ", ".join(day.isoformat() for day in zero_days)
        raise RuntimeError(f"Reviews data incomplete: zero rows on {z}. Aborting window {start_date}→{end_date}.")

    # 3) Summarizer (RUN ONCE)
    logger.info("Running summarizer for %d reviews...", review_length)
    wins, opps = generate_summaries(reviews)

    # 4) Tagger + Sentiment grader (RUN PER VERSION)
    sentiment_results = {}
    tagger_results = {}

    for mapping_version, keywords_file in VERSIONS:
        logger.info("Running tagger + sentiment for mapping_version=%s using %s", mapping_version, keywords_file)

        # Tagger
        tagged_rows = tag_and_load_review_tags(
            reviews_df,
            start_date,
            end_date,
            keywords_file=keywords_file,
            mapping_version=mapping_version,
        )
        tagger_results[mapping_version] = tagged_rows

        # Sentiment grader
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

    # 5) Store summaries
    summary_ok = load_summaries(start_date, end_date, wins, opps, review_length)

    return {
        "date_range": f"{start_date} to {end_date}",
        "review_count": review_length,
        "daily_counts": {d.isoformat(): cnt.get(d, 0) for d in all_days},
        "wins_summary": wins,
        "opps_summary": opps,
        "summary_status": "inserted" if summary_ok else "failed",
        "tagger_rows_by_version": tagger_results,
        "sentiment_status_by_version": sentiment_results,
    }


# ──────────────────────────────────────────────────────────────────────────────
# HTTP entrypoint (Cloud Run / Functions Framework)
# ──────────────────────────────────────────────────────────────────────────────
def summarize_and_load(request: Request):
    """
    If auto_loop is false:
      - runs 1 window (default Fri->Thu OR passed window)
    If auto_loop is true:
      - aligns the provided window to Fri->Thu boundaries
      - loops week-by-week and runs ALL weeks
    """
    try:
        # Window selection
        start_date, end_date = _get_override_window(request)
        if not (start_date and end_date):
            start_date, end_date = last_complete_fri_to_thu()

        start_date = _to_date(start_date)
        end_date = _to_date(end_date)
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            raise RuntimeError("Could not coerce start/end window to dates")

        if start_date > end_date:
            raise RuntimeError("start_date cannot be after end_date")

        auto_loop = _get_auto_loop_flag(request)

        # If NOT looping → original behavior
        if not auto_loop:
            body = process_window(start_date, end_date)
            return json.dumps(body), 200, {"Content-Type": "application/json"}

        # AUTO LOOP MODE
        aligned_start = _align_to_friday(start_date)
        aligned_end = _align_to_thursday(end_date)

        logger.info(
            "AUTO_LOOP enabled. Requested window: %s→%s. Aligned to Fri→Thu: %s→%s",
            start_date, end_date, aligned_start, aligned_end
        )

        results = []
        ok = 0
        fail = 0

        for ws, we in _iter_fri_thu_weeks(aligned_start, aligned_end):
            try:
                logger.info("AUTO_LOOP running week: %s→%s", ws, we)
                week_result = process_window(ws, we)
                results.append({"week": f"{ws}→{we}", "status": "success", "result": week_result})
                ok += 1
            except Exception as ex:
                logger.exception("AUTO_LOOP failed week %s→%s", ws, we)
                results.append({"week": f"{ws}→{we}", "status": "failed", "error": str(ex)})
                fail += 1

                # stop on first failure (safe default)
                stop_on_error = (os.getenv("STOP_ON_ERROR", "true").lower() == "true")
                if stop_on_error:
                    break

        resp = {
            "auto_loop": True,
            "requested_window": f"{start_date}→{end_date}",
            "aligned_window": f"{aligned_start}→{aligned_end}",
            "weeks_success": ok,
            "weeks_failed": fail,
            "stop_on_error": (os.getenv("STOP_ON_ERROR", "true").lower() == "true"),
            "weeks": results,
        }
        status_code = 200 if fail == 0 else 207  # 207 = multi-status
        return json.dumps(resp), status_code, {"Content-Type": "application/json"}

    except Exception as e:
        logger.exception("Unhandled error")
        return json.dumps({"error": str(e), "status": "failed"}), 500, {
            "Content-Type": "application/json"
        }


# ──────────────────────────────────────────────────────────────────────────────
# Local test runner
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Local test:
    #   AUTO_LOOP=true START_DATE=2025-01-01 END_DATE=2025-01-31 python main.py
    s, e = _get_override_window(request=None)
    if not (s and e):
        s, e = "2025-01-01", "2025-01-31"

    s = _to_date(s)
    e = _to_date(e)

    # simulate auto loop from env
    if os.getenv("AUTO_LOOP", "false").lower() == "true":
        aligned_start = _align_to_friday(s)
        aligned_end = _align_to_thursday(e)
        print(f"AUTO_LOOP: requested {s}→{e} aligned {aligned_start}→{aligned_end}")
        for ws, we in _iter_fri_thu_weeks(aligned_start, aligned_end):
            print(f"Running week {ws}→{we}")
            out = process_window(ws, we)
            print(out.get("date_range"), out.get("review_count"))
    else:
        out = process_window(s, e)
        print(out.get("date_range"), out.get("review_count"))
