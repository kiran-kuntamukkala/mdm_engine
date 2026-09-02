from functions.matcher import calculate_match_score, exact_match, fuzzy_match


def test_exact_match_for_email():
    assert exact_match("abc@gmail.com", "abc@gmail.com") is True


def test_email_score_is_full_for_exact_match():
    assert calculate_match_score("EMAIL", "abc@gmail.com", "abc@gmail.com") == 100.0


def test_fuzzy_name_match_is_positive():
    assert fuzzy_match("Robert Smith", "Rob Smith") > 70
