import re


# Patterns compiled once at module load for efficiency
_MARKDOWN_BOLD_ITALIC = re.compile(r"\*{1,3}(.*?)\*{1,3}")   # **bold**, *italic*, ***both***
_MARKDOWN_STRIKETHROUGH = re.compile(r"~~(.*?)~~")
_MARKDOWN_INLINE_CODE = re.compile(r"`[^`]*`")
_MARKDOWN_QUOTE = re.compile(r"^>+\s?", re.MULTILINE)        # > quoted lines
_MARKDOWN_HEADER = re.compile(r"^#{1,6}\s+", re.MULTILINE)   # # Heading
_URL = re.compile(r"https?://\S+|www\.\S+")
_SPECIAL_CHARS = re.compile(r"[^a-zA-Z0-9\s\u0900-\u097F.,!?']")  # keep Devanagari for language detection
_WHITESPACE = re.compile(r"\s+")

# Exact strings Reddit inserts when content is unavailable
_REDDIT_PLACEHOLDERS = {"[deleted]", "[removed]", "[ Removed by Reddit ]"}


def clean(text: str) -> str:
    """
    Steps:
        1. Handle Reddit placeholders ([deleted], [removed])
        2. Strip Reddit markdown (bold, italic, strikethrough, quotes, headers, inline code)
        3. Remove URLs
        4. Remove special characters (keep letters, digits, Devanagari, and .!?',)
        5. Normalize whitespace (collapse spaces and newlines)
        6. Trim leading/trailing whitespace
    """
    if not text:
        return ""

    # Step 1 — Reddit placeholders
    if text.strip() in _REDDIT_PLACEHOLDERS:
        return ""

    # Step 2 — Strip Reddit markdown
    text = _MARKDOWN_BOLD_ITALIC.sub(r"\1", text)    # keep inner text
    text = _MARKDOWN_STRIKETHROUGH.sub(r"\1", text)  # keep inner text
    text = _MARKDOWN_INLINE_CODE.sub("", text)       # drop inline code entirely
    text = _MARKDOWN_QUOTE.sub("", text)             # drop quote prefix characters
    text = _MARKDOWN_HEADER.sub("", text)            # drop heading hashes

    # Step 3 — Remove URLs
    text = _URL.sub("", text)

    # Step 4 — Remove special characters
    # Keeps: a-z A-Z 0-9 whitespace Devanagari (for language_filter) . , ! ? '
    text = _SPECIAL_CHARS.sub(" ", text)

    # Step 5 — Normalize whitespace
    text = _WHITESPACE.sub(" ", text)

    # Step 6 — Trim
    text = text.strip()

    return text


if __name__ == "__main__":
    samples = [
        "**Check this out** — great post! https://example.com",
        "[deleted]",
        "[removed]",
        "~~strikethrough~~ and *italic* text here",
        "> This is a quoted line\nAnd this is a reply",
        "Normal text with    extra   spaces\n\nand newlines",
        "Text with émojis 🔥 and $pecial ch@racters!!!",
        "Hello नमस्ते this is Hinglish",
        "",
        None,
    ]

    print("=" * 60)
    print("cleaner.py — manual test")
    print("=" * 60)
    for s in samples:
        result = clean(s)
        print(f"IN:  {repr(s)}")
        print(f"OUT: {repr(result)}")
        print("-" * 40)
