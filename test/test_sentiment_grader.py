import pytest
from collections import defaultdict
from app.sentiment_grader import count_category_mentions  # adjust path to your module


@pytest.fixture
def sample_category_keywords():
    return {
        "Customer Service": ["staff", "service", "helpful", "rude"],
        "Wait Time": ["wait", "delay", "appointment"],
        "Pricing": ["expensive", "cheap", "price"],
    }


@pytest.fixture
def sample_reviews():
    return [
        {"review_comment": "The staff was very helpful and kind!"},
        {"review_comment": "Had to wait too long for my appointment."},
        {"review_comment": "Service was okay, but prices were expensive."},
        {"review_comment": "Loved the helpful staff and fast service."},
    ]


def test_basic_counts(sample_reviews, sample_category_keywords):
    result = count_category_mentions(sample_reviews, sample_category_keywords)
    assert result == {
        "Customer Service": 2,
        "Wait Time": 1,
        "Pricing": 1,
    }


def test_empty_reviews(sample_category_keywords):
    result = count_category_mentions([], sample_category_keywords)
    assert result == {}


def test_no_keywords_match(sample_category_keywords):
    reviews = [{"review_comment": "The food was delicious and the music was nice."}]
    result = count_category_mentions(reviews, sample_category_keywords)
    assert result == {}


def test_case_insensitive(sample_category_keywords):
    reviews = [{"review_comment": "The STAFF was AWESOME!"}]
    result = count_category_mentions(reviews, sample_category_keywords)
    assert result == {"Customer Service": 1}


def test_multiple_mentions_in_same_review(sample_category_keywords):
    reviews = [{"review_comment": "The service and staff were both helpful."}]
    result = count_category_mentions(reviews, sample_category_keywords)
    # Should count only once per category even if multiple keywords appear
    assert result == {"Customer Service": 1}


def test_non_dict_reviews(sample_category_keywords):
    reviews = [
        "The staff was rude and unhelpful.",
        "Prices are cheap and affordable."
    ]
    result = count_category_mentions(reviews, sample_category_keywords)
    assert result == {"Customer Service": 1, "Pricing": 1}


def test_partial_word_does_not_match(sample_category_keywords):
    reviews = [{"review_comment": "The staffer was great."}]  # "staffer" should NOT match "staff"
    result = count_category_mentions(reviews, sample_category_keywords)
    assert result == {}
