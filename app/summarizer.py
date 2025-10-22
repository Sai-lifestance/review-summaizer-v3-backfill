import logging
import json
import datetime
from app.clients import openai_client, bq_client
from app.config import BQ_PROJECT_SUMMARIES, SUMMARY_TABLE, DEFAULT_MODEL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def generate_summaries(reviews,model=DEFAULT_MODEL,output_response=False):

    if not reviews:
        return "No new reviews."
    
    review_texts = "\n".join([f"- {r['review_comment']}" for r in reviews if r['review_comment']])

    # Wins
    prompt_wins = f"""
    Summarize the following reviews.
    Focus ONLY on wins: praise, highlights, positive themes.
    Include context on why the wins occurred based on the reviews.
    Please output only the TOP 3 most impactful in bullet point format.
    Make sure a new line character is included in between each bullet point item.
    Make sure the 3 bullet point items are in order of descending prominence. 
    Reviews:
    {review_texts}
    """

    wins_response = openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are an expert summarizer of reviews for business insights."},
            {"role": "user", "content": prompt_wins}
        ]
    )

    wins_text = wins_response.choices[0].message.content

    # Opportunities
    prompt_opps = f"""
    Summarize the following reviews.
    Focus ONLY on opportunities: complaints, concerns, and areas for improvement.
    Include context on why the complaints and concerns occurred based on the reviews.
    Please output only the TOP 3 most impactful in bullet point format.
    Make sure a new line character is included in between each bullet point item.
    Make sure the 3 bullet point items are in order of descending prominence. 
    {review_texts}
    """

    logger.info("Requesting response from OpenAI...")

    opps_response = openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are an expert summarizer of reviews for business insights."},
            {"role": "user", "content": prompt_opps}
        ]
    )

    opps_text = opps_response.choices[0].message.content

    if output_response:
        try:
            with open("tmp/full_response_wins.txt", "w", encoding="utf-8") as f:
                f.write(json.dumps(wins_response.model_dump(), indent=2))

            with open("tmp/full_response_opps.txt", "w", encoding="utf-8") as f:
                f.write(json.dumps(opps_response.model_dump(), indent=2))

            with open("tmp/response_wins.txt", "w", encoding="utf-8") as f:
                f.write(wins_text)

            with open("tmp/response_opps.txt", "w", encoding="utf-8") as f:
                f.write(opps_text)

        except Exception as e:
            print("Error occurred:", e)

    return wins_text, opps_text

def load_summaries(start_date, end_date, wins, opps, review_count):
    table_id = f"{BQ_PROJECT_SUMMARIES}.{SUMMARY_TABLE}"

    row = [{
        "week_start": start_date,
        "week_end": end_date,
        "wins_summary": wins,
        "opps_summary": opps,
        "review_count": review_count,
        "insert_timestamp_utc": datetime.datetime.now().isoformat()
    }]

    logger.info(f"Loading generated summary into {table_id}...")

    errors = bq_client.insert_rows_json(table_id, row)
    if errors:
        print("Error inserting summary: ", errors)
        return False
    else:
        print("Summaries inserted successfully.")
        return True
    
if __name__ == "__main__":
    from app.utils import get_reviews
    reviews = get_reviews("2025-10-01", "2025-10-01")
    generated_summary = generate_summaries(reviews, output_response=True)
    print(generated_summary)