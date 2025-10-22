from google.cloud import bigquery
from openai import OpenAI
from app.config import BQ_PROJECT_REVIEWS, OPENAI_API_KEY

bq_client = bigquery.Client(project=BQ_PROJECT_REVIEWS)
openai_client = OpenAI(api_key=OPENAI_API_KEY)