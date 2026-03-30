-- db/schema.sql

DROP TABLE IF EXISTS reddit_posts;

CREATE TABLE IF NOT EXISTS reddit_posts (
    id                TEXT PRIMARY KEY,       -- Reddit post ID — the deduplication key
    title             TEXT NOT NULL,
    selftext          TEXT,                   -- Empty string for link-only posts
    score             INTEGER,
    permalink	      TEXT,                  -- Link for the reddit url
    created_utc       TIMESTAMP,             -- When the post was created on Reddit (UTC) 
    collected_at      TIMESTAMP NOT NULL,    -- When our pipeline first captured it
    is_english        BOOLEAN DEFAULT TRUE,
    cleaned_text      TEXT,                   -- Preprocessed version for sentiment model
    sentiment_score   FLOAT,                  -- NULL until scored (0.0 = negative, 1.0 = positive)
    sentiment_label   TEXT,                   -- 'positive', 'neutral', or 'negative'
    scored_at         TIMESTAMP               -- NULL until scored
);
