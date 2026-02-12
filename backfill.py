import os
import sys
from datetime import datetime, timedelta, date

# REQUIRED
BACKFILL_START = os.getenv("BACKFILL_START")  # YYYY-MM-DD
BACKFILL_END = os.getenv("BACKFILL_END")      # YYYY-MM-DD

# OPTIONAL
STOP_ON_ERROR = os.getenv("STOP_ON_ERROR", "true").lower() == "true"
ALIGN_TO_FRI_THU = os.getenv("ALIGN_TO_FRI_THU", "true").lower() == "true"


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def align_to_friday(d: date) -> date:
    # Fri = 4 (Mon=0..Sun=6)
    shift = (d.weekday() - 4) % 7
    return d - timedelta(days=shift)


def align_to_thursday(d: date) -> date:
    # Thu = 3
    shift = (3 - d.weekday()) % 7
    return d + timedelta(days=shift)


def iter_weeks(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=6), end)  # 7-day chunk
        cur += timedelta(days=7)


def main():
    if not BACKFILL_START or not BACKFILL_END:
        print("Missing BACKFILL_START / BACKFILL_END (YYYY-MM-DD)")
        print("Example:")
        print("  export BACKFILL_START=2025-10-31")
        print("  export BACKFILL_END=2025-12-04")
        sys.exit(2)

    start = parse_ymd(BACKFILL_START)
    end = parse_ymd(BACKFILL_END)

    if start > end:
        raise ValueError("BACKFILL_START cannot be after BACKFILL_END")

    if ALIGN_TO_FRI_THU:
        start = align_to_friday(start)
        end = align_to_thursday(end)

    print("=== BACKFILL ===")
    print(f"Window: {ymd(start)} -> {ymd(end)}")
    print(f"ALIGN_TO_FRI_THU={ALIGN_TO_FRI_THU} STOP_ON_ERROR={STOP_ON_ERROR}")
    print("===============")

    # IMPORTANT: this imports your existing pipeline code from main.py
    import main as app

    ok = 0
    fail = 0

    for ws, we in iter_weeks(start, end):
        s = ymd(ws)
        e = ymd(we)
        print(f"\n--- Running: {s} -> {e} ---")

        try:
            result = app.run_pipeline_for_window(s, e)
            ok += 1
            print(f"✅ Success: {s} -> {e} | {result}")
        except Exception as ex:
            fail += 1
            print(f"❌ Failed: {s} -> {e} | {repr(ex)}")
            if STOP_ON_ERROR:
                raise

    print("\n=== DONE ===")
    print(f"Success: {ok}")
    print(f"Failed:  {fail}")
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
