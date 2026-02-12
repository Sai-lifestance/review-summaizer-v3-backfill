import os

# ---- BigQuery Config ----
BQ_PROJECT_REVIEWS = "ls-prod-warehouse" 
REVIEWS_TABLE = "marketing_marts.fct_review_responses" 

BQ_PROJECT_SUMMARIES = "ls-raw-dev"
SUMMARY_TABLE = "ai_generated_outputs.review_summaries"
SENTIMENT_GRADE_TABLE = "ai_generated_outputs.review_sentiment_grades"   
REVIEW_TAGS_TABLE = "ai_generated_outputs.tagged_reviews"

# ---- OpenAI Config ----
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")

# ---- Keywords for Sentiment Grader ----

REVIEW_CATEGORIES = ['Clinical Care and Outcomes', 'Patient Experience', 'Billing', 'Intake Experience', 'Technology']


