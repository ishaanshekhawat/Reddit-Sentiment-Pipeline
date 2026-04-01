import re
import logging
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

# Make langdetect deterministic
DetectorFactory.seed = 42

logger = logging.getLogger(__name__)

# Unicode range for Devanagari script (Hindi, Marathi, Nepali, Sanskrit)
_DEVANAGARI_PATTERN = re.compile(r"[\u0900-\u097F]")

# Minimum character count to attempt language detection.
# Short strings (< 20 chars) cause langdetect to return unreliable results.
_MIN_LENGTH_FOR_DETECTION = 20

# langdetect confidence threshold above which we trust a non-English result.
# Set to 0.85 — below this, we give the post the benefit of the doubt and treat it as English (common for Hinglish posts that confuse the detector).
_NON_ENGLISH_CONFIDENCE_THRESHOLD = 0.85


def is_devanagari(text: str) -> bool:
    """
    Returns True if the text contains any Devanagari Unicode characters.

    This is a fast, regex-based check that runs before the slower langdetect call. Devanagari posts are definitively non-English and should be excluded
    from sentiment scoring regardless of langdetect's output.
    """
    return bool(_DEVANAGARI_PATTERN.search(text))


def is_english(text: str) -> bool:
    """
    Determines whether a post's text is English (or Hinglish) and suitable for sentiment scoring.

    Decision logic (in order):
        1. Empty or very short text → treat as English (True) so it gets stored; the sentiment model will handle it gracefully.
        2. Contains Devanagari characters → definitely not English (False).
        3. langdetect identifies language:
             - 'en' → English (True)
             - anything else with confidence >= 0.85 → not English (False)
             - anything else with confidence < 0.85 → treat as English (True) (catches Hinglish, which langdetect often misidentifies)
        4. langdetect throws an exception (too short, no features) → treat as English (True) to avoid incorrectly discarding posts.

    Returns:
        True  → post should be stored with is_english=TRUE and scored
        False → post should be stored with is_english=FALSE, not scored
    """
    if not text or len(text.strip()) < _MIN_LENGTH_FOR_DETECTION:
        return True

    # Fast path — Devanagari check
    if is_devanagari(text):
        logger.debug("Devanagari detected — marking as non-English.")
        return False

    # Slow path — langdetect
    try:
        from langdetect import detect_langs
        results = detect_langs(text)

        if not results:
            return True

        # detect_langs returns a list of lang:prob pairs sorted by probability
        top = results[0]
        lang = top.lang
        prob = top.prob

        if lang == "en":
            return True

        # Non-English with high confidence → exclude
        if prob >= _NON_ENGLISH_CONFIDENCE_THRESHOLD:
            logger.debug(
                f"Non-English detected: lang={lang}, confidence={prob:.2f} "
                f"(>= threshold {_NON_ENGLISH_CONFIDENCE_THRESHOLD}) — excluding."
            )
            return False

        # Non-English with low confidence → likely Hinglish, treat as English
        logger.debug(
            f"Ambiguous language: lang={lang}, confidence={prob:.2f} "
            f"(< threshold {_NON_ENGLISH_CONFIDENCE_THRESHOLD}) — treating as English."
        )
        return True

    except LangDetectException as e:
        # langdetect raises this when text has no detectable features
        # (e.g. all numbers, all punctuation, extremely short after cleaning)
        logger.debug(f"LangDetectException — defaulting to English. Error: {e}")
        return True


def get_language_label(text: str) -> str:
    """
    Returns a human-readable language label for logging/debugging purposes.
    Not used in the main pipeline — useful for manual inspection.
    """
    if not text or len(text.strip()) < _MIN_LENGTH_FOR_DETECTION:
        return "english"

    if is_devanagari(text):
        return "devanagari"

    try:
        lang = detect(text)
        if lang == "en":
            return "english"

        from langdetect import detect_langs
        results = detect_langs(text)
        if results and results[0].prob < _NON_ENGLISH_CONFIDENCE_THRESHOLD:
            return "hinglish"

        return f"other:{lang}"

    except LangDetectException:
        return "english"


if __name__ == "__main__":
    samples = [
        ("English post", "Had a really fun weekend in Mumbai, anyone else go to Bandra?"),
        ("Hinglish post", "Bhai kal party thi, bahut maza aaya yaar seriously"),
        ("Hindi Devanagari", "आज बहुत अच्छा दिन था, मुंबई में घूमा"),
        ("Mixed Devanagari", "Hello everyone, आज का दिन अच्छा था"),
        ("Short text", "Hello"),
        ("Empty string", ""),
        ("Numbers only", "123 456 789"),
        ("English formal", "The infrastructure development in Indian cities has accelerated significantly."),
    ]

    print("=" * 70)
    print("language_filter.py — manual test")
    print("=" * 70)
    for label, text in samples:
        result = is_english(text)
        lang_label = get_language_label(text)
        status = "✓ ENGLISH" if result else "✗ NON-ENGLISH"
        print(f"[{label}]")
        print(f"  Text:   {repr(text[:60])}")
        print(f"  Result: {status} | Label: {lang_label}")
        print()
