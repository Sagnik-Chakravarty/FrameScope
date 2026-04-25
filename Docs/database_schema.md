Database Schema — FrameScope

Overview
--------

The pipeline consists of three core tables:

reddit_posts            → Raw Reddit posts
reddit_sentence_items   → Sentence-level extracted AI-related text
llm_labels              → LLM-generated annotations (metaphor, stance, granularity)

Pipeline Flow:

reddit_posts
    ↓
reddit_sentence_items
    ↓
llm_labels


------------------------------------------------------------
1. reddit_posts
------------------------------------------------------------

Stores raw Reddit posts.

Columns:

- source (TEXT): Data source ("reddit")
- post_id (TEXT): Unique Reddit post ID
- subreddit (TEXT): Subreddit name
- author (TEXT): Post author
- created_utc (INTEGER): Timestamp (Unix)
- created_datetime (TEXT): Human-readable datetime
- title (TEXT): Post title
- selftext (TEXT): Post body
- text (TEXT): Combined title + body
- score (INTEGER): Reddit score
- num_comments (INTEGER): Number of comments
- url (TEXT): Post URL
- permalink (TEXT): Reddit permalink
- raw_file (TEXT): Source file reference
- inserted_at (TIMESTAMP): Insert time

Primary Key:
(source, post_id)


------------------------------------------------------------
2. reddit_sentence_items
------------------------------------------------------------

Stores sentence-level units extracted from posts that contain AI-related content.

Columns:

- source (TEXT): Data source ("reddit")
- sentence_id (TEXT): Unique sentence ID
- post_id (TEXT): Parent post ID
- subreddit (TEXT): Subreddit name
- created_utc (INTEGER): Timestamp
- created_datetime (TEXT): Datetime
- preceding_sentence (TEXT): Previous sentence
- ai_sentence (TEXT): Target sentence (AI-related)
- subsequent_sentence (TEXT): Next sentence
- context_text (TEXT): Combined context text
- score (INTEGER): Post score
- raw_file (TEXT): Source file reference
- inserted_at (TIMESTAMP): Insert time

Primary Key:
(source, sentence_id)

Foreign Key:
(source, post_id) → reddit_posts(source, post_id)


------------------------------------------------------------
3. llm_labels
------------------------------------------------------------

Stores LLM-generated annotations for each sentence.

Columns:

- source (TEXT): Data source ("reddit")
- sentence_id (TEXT): Sentence ID
- metaphor_category (TEXT): Dominant metaphor
- metaphor_present (INTEGER): 1 if metaphor exists, else 0
- granularity (TEXT): AI scope (General-AI, Model-Specific, Domain-Specific, Not Applicable)
- stance (TEXT): Sentiment (Positive, Neutral/Unclear, Negative)
- confidence (REAL): Reserved
- reasoning (TEXT): Reserved
- model_name (TEXT): LLM used (e.g., llama3.2)
- labeled_at (TIMESTAMP): Label timestamp

Primary Key:
(source, sentence_id)

Foreign Key:
(source, sentence_id) → reddit_sentence_items(source, sentence_id)


------------------------------------------------------------
Relationships
------------------------------------------------------------

reddit_posts (1) → (many) reddit_sentence_items  
reddit_sentence_items (1) → (1) llm_labels  


------------------------------------------------------------
Data Flow
------------------------------------------------------------

1. reddit_posts
2. sentence preprocessing
3. reddit_sentence_items
4. LLM labeling (05_label_llm.py)
5. llm_labels


------------------------------------------------------------
Example Join
------------------------------------------------------------

SELECT
    r.subreddit,
    r.created_utc,
    r.ai_sentence,
    l.metaphor_category,
    l.granularity,
    l.stance
FROM reddit_sentence_items r
JOIN llm_labels l
    ON r.sentence_id = l.sentence_id
WHERE l.source = 'reddit';


------------------------------------------------------------
Notes
------------------------------------------------------------

- All annotations are sentence-level
- The pipeline is idempotent (safe to rerun)
- Only structured outputs are stored in the database
- Raw LLM outputs are logged separately


------------------------------------------------------------
Future Extensions
------------------------------------------------------------

- Aggregated tables (monthly / subreddit-level)
- Model version tracking
- Confidence scores
- Multi-label metaphors (if needed)
