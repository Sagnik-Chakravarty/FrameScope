import html

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import create_engine


PANEL_CONTENT_HEIGHT = 300
PANEL_FRAME_EXTRA_HEIGHT = 120
PANEL_OUTER_HEIGHT = PANEL_CONTENT_HEIGHT + PANEL_FRAME_EXTRA_HEIGHT
PANEL_COMPONENT_HEIGHT = PANEL_CONTENT_HEIGHT + 20


@st.cache_resource
def get_engine():
    neon_url = st.secrets.get("NeonDb")

    if not neon_url:
        st.error("NeonDb secret not found. Add it in Streamlit Cloud → Settings → Secrets.")
        st.stop()
    return create_engine(neon_url, pool_pre_ping=True)


METAPHOR_DEFINITIONS = {
    "Tool": "AI is described as something people use to complete a task.",
    "Assistant": "AI is described as a helper that supports the user.",
    "Genie": "AI is described as magical, wish-granting, or unusually powerful.",
    "Mirror": "AI is described as reflecting human behavior, knowledge, or bias.",
    "Child": "AI is described as something that learns, develops, or needs guidance.",
    "Friend": "AI is described as a companion or social partner.",
    "Animal": "AI is described as something trainable, instinctive, or dangerous.",
    "God": "AI is described as all-knowing, extremely powerful, or beyond human ability.",
    "None": "No clear metaphor is being used.",
}

GRANULARITY_DEFINITIONS = {
    "General-AI": "Comments about AI in general, including broad benefits, problems, or social impact.",
    "Model-Specific": "Comments about a specific AI model, tool, or company system, such as ChatGPT, Claude, Gemini, or Copilot.",
    "Domain-Specific": "Comments about AI in a specific field or use case, such as education, art, science, programming, or healthcare.",
    "Not Applicable": "The text is not meaningfully about AI.",
}

STANCE_DEFINITIONS = {
    "Positive": "Positive toward AI or toward the specific category being discussed.",
    "Neutral/Unclear": "A question, report, unclear comment, mixed comment, or not applicable.",
    "Negative": "Negative, concerned, frustrated, or critical toward AI or the selected category.",
}


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def pct_delta(current: float, previous: float) -> str:
    if previous == 0:
        return "0"
    return f"{(current - previous) / previous:+.2%}"


@st.cache_data(ttl=60)
def get_latest_week_overall_summary() -> str:
    engine = get_engine()

    query = """
    SELECT summary_text
    FROM weekly_llm_summary
    WHERE scope = 'overall'
    ORDER BY week_start DESC
    LIMIT 1;
    """

    df = pd.read_sql_query(query, engine)

    if df.empty or pd.isna(df.loc[0, "summary_text"]):
        return "No weekly summary available yet."

    return str(df.loc[0, "summary_text"])


@st.cache_data(ttl=60)
def get_available_subreddits() -> list[str]:
    engine = get_engine()

    query = """
    SELECT DISTINCT subreddit
    FROM aggregate_weekly_metrics
    WHERE subreddit IS NOT NULL
      AND TRIM(subreddit) != ''
    ORDER BY subreddit;
    """

    df = pd.read_sql_query(query, engine)
    return df["subreddit"].tolist()


@st.cache_data(ttl=60)
def get_metaphor_examples() -> dict:
    engine = get_engine()

    query = """
    WITH ranked AS (
        SELECT
            metaphor_category,
            ai_sentence,
            ROW_NUMBER() OVER (
                PARTITION BY metaphor_category
                ORDER BY score DESC, period_start DESC
            ) AS rn
        FROM polarizing_examples
        WHERE metaphor_category IS NOT NULL
          AND TRIM(metaphor_category) != ''
          AND ai_sentence IS NOT NULL
          AND TRIM(ai_sentence) != ''
    )

    SELECT
        metaphor_category,
        ai_sentence
    FROM ranked
    WHERE rn = 1;
    """

    df = pd.read_sql_query(query, engine)
    return dict(zip(df["metaphor_category"], df["ai_sentence"]))


def animated_metric_card(
    title: str,
    rows: pd.DataFrame,
    key: str,
    seconds_per_item: int = 6,
) -> None:
    safe_key = key.replace("-", "_")

    if rows.empty:
        rows = pd.DataFrame(
            [{"label": "No data", "current_n": 0, "previous_n": 0}]
        )

    temp = rows.copy()
    temp["current_n"] = temp["current_n"].fillna(0).astype(int)
    temp["previous_n"] = temp["previous_n"].fillna(0).astype(int)
    temp["delta"] = temp.apply(
        lambda r: pct_delta(r["current_n"], r["previous_n"]),
        axis=1,
    )

    rows_json = temp[["label", "current_n", "delta"]].to_json(orient="records")
    interval_ms = seconds_per_item * 1000

    html_block = f"""
    <div class="metric-card">
        <div id="{safe_key}_title" class="metric-title">{title}</div>
        <div id="{safe_key}_value" class="metric-value">0</div>
        <div id="{safe_key}_delta" class="metric-delta">0</div>
    </div>

    <style>
    .metric-card {{
        padding: 0.35rem 0 0 0;
        background: transparent;
        min-height: 125px;
        font-family: sans-serif;
        overflow: hidden;
    }}

    .metric-title {{
        font-size: 0.875rem;
        color: rgba(49, 51, 63, 0.75);
        margin-bottom: 0.25rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        opacity: 1;
        transition: opacity 0.35s ease, transform 0.35s ease;
    }}

    .metric-value {{
        font-size: 2rem;
        font-weight: 600;
        color: rgb(49, 51, 63);
        line-height: 1.2;
        opacity: 1;
        transition: opacity 0.35s ease, transform 0.35s ease;
    }}

    .metric-delta {{
        display: inline-block;
        font-size: 0.85rem;
        color: rgb(107, 114, 128);
        background: rgba(107, 114, 128, 0.14);
        border-radius: 999px;
        padding: 0.1rem 0.4rem;
        margin-top: 0.25rem;
        opacity: 1;
        transition: opacity 0.35s ease, transform 0.35s ease;
    }}

    .metric-delta.delta-positive {{
        color: rgb(9, 171, 59);
        background: rgba(9, 171, 59, 0.12);
    }}

    .metric-delta.delta-negative {{
        color: rgb(225, 29, 72);
        background: rgba(225, 29, 72, 0.12);
    }}

    .metric-delta.delta-neutral {{
        color: rgb(107, 114, 128);
        background: rgba(107, 114, 128, 0.14);
    }}

    .fade-out {{
        opacity: 0;
        transform: translateY(6px);
    }}
    </style>

    <script>
    const rows_{safe_key} = {rows_json};
    let idx_{safe_key} = 0;

    const titleEl_{safe_key} = document.getElementById("{safe_key}_title");
    const valueEl_{safe_key} = document.getElementById("{safe_key}_value");
    const deltaEl_{safe_key} = document.getElementById("{safe_key}_delta");

    function formatNumber_{safe_key}(x) {{
        return Number(x || 0).toLocaleString();
    }}

    function applyDeltaClass_{safe_key}(deltaText) {{
        deltaEl_{safe_key}.classList.remove(
            "delta-positive",
            "delta-negative",
            "delta-neutral"
        );

        const n = Number.parseFloat(String(deltaText).replace('%', ''));

        if (Number.isNaN(n) || n === 0) {{
            deltaEl_{safe_key}.classList.add("delta-neutral");
        }} else if (n > 0) {{
            deltaEl_{safe_key}.classList.add("delta-positive");
        }} else {{
            deltaEl_{safe_key}.classList.add("delta-negative");
        }}
    }}

    function updateMetric_{safe_key}() {{
        const row = rows_{safe_key}[idx_{safe_key}];

        titleEl_{safe_key}.classList.add("fade-out");
        valueEl_{safe_key}.classList.add("fade-out");
        deltaEl_{safe_key}.classList.add("fade-out");

        setTimeout(function () {{
            titleEl_{safe_key}.innerText = "{title} - " + row.label;
            valueEl_{safe_key}.innerText = formatNumber_{safe_key}(row.current_n);
            deltaEl_{safe_key}.innerText = row.delta;
            applyDeltaClass_{safe_key}(row.delta);

            titleEl_{safe_key}.classList.remove("fade-out");
            valueEl_{safe_key}.classList.remove("fade-out");
            deltaEl_{safe_key}.classList.remove("fade-out");

            idx_{safe_key} = (idx_{safe_key} + 1) % rows_{safe_key}.length;
        }}, 350);
    }}

    updateMetric_{safe_key}();
    setInterval(updateMetric_{safe_key}, {interval_ms});
    </script>
    """

    components.html(html_block, height=150)


# --------------------------------------------------
# DATA QUERIES
# --------------------------------------------------

@st.cache_data(ttl=60)
def get_home_metrics() -> dict:
    engine = get_engine()

    query = """
    WITH latest_week AS (
        SELECT MAX(week_start) AS latest_week_start
        FROM aggregate_weekly_metrics
    ),

    previous_week AS (
        SELECT MAX(week_start) AS previous_week_start
        FROM aggregate_weekly_metrics
        WHERE week_start < (SELECT latest_week_start FROM latest_week)
    )

    SELECT
        COALESCE(SUM(n_items), 0) AS total_posts,

        COALESCE(SUM(CASE
            WHEN week_start < (SELECT latest_week_start FROM latest_week)
            THEN n_items ELSE 0
        END), 0) AS total_excl_last_7_days,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT latest_week_start FROM latest_week)
            THEN n_items ELSE 0
        END), 0) AS last_7_days_posts,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT previous_week_start FROM previous_week)
            THEN n_items ELSE 0
        END), 0) AS prev_7_days_posts

    FROM aggregate_weekly_metrics;
    """

    df = pd.read_sql_query(query, engine)

    if df.empty:
        return {
            "total_posts": 0,
            "total_excl_last_7_days": 0,
            "last_7_days_posts": 0,
            "prev_7_days_posts": 0,
        }

    return df.iloc[0].fillna(0).to_dict()


@st.cache_data(ttl=60)
def get_metaphor_cycle_metrics() -> pd.DataFrame:
    engine = get_engine()

    query = """
    WITH latest_week AS (
        SELECT MAX(week_start) AS latest_week_start
        FROM aggregate_weekly_metrics
    ),

    previous_week AS (
        SELECT MAX(week_start) AS previous_week_start
        FROM aggregate_weekly_metrics
        WHERE week_start < (SELECT latest_week_start FROM latest_week)
    )

    SELECT
        metaphor_category AS label,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT latest_week_start FROM latest_week)
            THEN n_items ELSE 0
        END), 0) AS current_n,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT previous_week_start FROM previous_week)
            THEN n_items ELSE 0
        END), 0) AS previous_n

    FROM aggregate_weekly_metrics
    WHERE metaphor_category IS NOT NULL
    GROUP BY metaphor_category
    ORDER BY current_n DESC;
    """

    df = pd.read_sql_query(query, engine)
    return df.fillna(0)


@st.cache_data(ttl=60)
def get_granularity_cycle_metrics() -> pd.DataFrame:
    engine = get_engine()

    query = """
    WITH latest_week AS (
        SELECT MAX(week_start) AS latest_week_start
        FROM aggregate_weekly_metrics
    ),

    previous_week AS (
        SELECT MAX(week_start) AS previous_week_start
        FROM aggregate_weekly_metrics
        WHERE week_start < (SELECT latest_week_start FROM latest_week)
    )

    SELECT
        granularity AS label,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT latest_week_start FROM latest_week)
            THEN n_items ELSE 0
        END), 0) AS current_n,

        COALESCE(SUM(CASE
            WHEN week_start = (SELECT previous_week_start FROM previous_week)
            THEN n_items ELSE 0
        END), 0) AS previous_n

    FROM aggregate_weekly_metrics
    WHERE granularity IS NOT NULL
    GROUP BY granularity
    ORDER BY current_n DESC;
    """

    df = pd.read_sql_query(query, engine)
    return df.fillna(0)


@st.cache_data(ttl=60)
def get_metaphor_time_series() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        week_start,
        metaphor_category,
        COALESCE(SUM(n_items), 0) AS n_posts
    FROM aggregate_weekly_metrics
    WHERE metaphor_category IS NOT NULL
      AND TRIM(metaphor_category) != ''
    GROUP BY
        week_start,
        metaphor_category
    ORDER BY
        week_start,
        metaphor_category;
    """

    df = pd.read_sql_query(query, engine)

    if not df.empty:
        df["week_start"] = pd.to_datetime(df["week_start"])

    return df


# --------------------------------------------------
# COMPONENTS
# --------------------------------------------------

def page_button() -> None:
    left, middle, right = st.columns([1, 1.4, 1])
    with middle:
        go_clicked = st.button(
            "Go To Dashboard",
            icon=":material/arrow_forward:",
            use_container_width=True,
        )

    if go_clicked:
        st.switch_page("Pages/02_reddit.py")


def methodology_box() -> None:
    pane_order = ["subreddits", "annotations", "metaphors"]
    pane_titles = {
        "subreddits": "Available Subreddits",
        "annotations": "Granularity and Stance",
        "metaphors": "Metaphor Labels",
    }

    if "methodology_pane_idx" not in st.session_state:
        existing_pane = st.session_state.get("methodology_pane", "subreddits")
        st.session_state.methodology_pane_idx = (
            pane_order.index(existing_pane)
            if existing_pane in pane_order
            else 0
        )

    with st.container(border=True, height=PANEL_OUTER_HEIGHT):
        st.markdown(
            """
            <style>
            .st-key-method_prev button,
            .st-key-method_next button {
                padding: 0 !important;
                min-height: 2rem;
            }

            .st-key-method_prev button p,
            .st-key-method_next button p {
                margin: 0 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        title_col, prev_col, next_col = st.columns([0.74, 0.13, 0.13])

        with title_col:
            st.markdown("#### Project Codebook")

        with prev_col:
            if st.button("◀", key="method_prev", use_container_width=True):
                st.session_state.methodology_pane_idx = (
                    st.session_state.methodology_pane_idx - 1
                ) % len(pane_order)

        with next_col:
            if st.button("▶", key="method_next", use_container_width=True):
                st.session_state.methodology_pane_idx = (
                    st.session_state.methodology_pane_idx + 1
                ) % len(pane_order)

        pane = pane_order[st.session_state.methodology_pane_idx]
        st.session_state.methodology_pane = pane
        st.caption(f"Section: {pane_titles[pane]}")

        if pane == "subreddits":
            subreddits = get_available_subreddits()

            body = "<h4>Available Subreddits</h4><ul>"
            for sub in subreddits:
                sub_clean = html.escape(sub)
                body += (
                    f'<li><a href="https://www.reddit.com/r/{sub_clean}" '
                    f'target="_blank">r/{sub_clean}</a></li>'
                )
            body += "</ul>"

        elif pane == "annotations":
            body = "<h4>Granularity</h4><ul>"
            for label, definition in GRANULARITY_DEFINITIONS.items():
                body += f"<li><b>{html.escape(label)}:</b> {html.escape(definition)}</li>"
            body += "</ul>"

            body += "<h4>Stance</h4><ul>"
            for label, definition in STANCE_DEFINITIONS.items():
                body += f"<li><b>{html.escape(label)}:</b> {html.escape(definition)}</li>"
            body += "</ul>"

        else:
            examples = get_metaphor_examples()

            body = "<h4>Metaphor Labels</h4><ul>"
            for label, definition in METAPHOR_DEFINITIONS.items():
                example = examples.get(
                    label,
                    "No example currently available in the labeled data.",
                )

                body += f"""
                <li style="margin-bottom:12px;">
                    <b>{html.escape(label)}:</b> {html.escape(definition)}
                    <br>
                    <span style="color:#6b7280;">
                        <i>Example:</i> {html.escape(str(example)[:240])}
                    </span>
                </li>
                """
            body += "</ul>"

        components.html(
            f"""
            <div style="
                height:{PANEL_CONTENT_HEIGHT}px;
                overflow-y:auto;
                padding:0 4px 0 0;
                border:none;
                border-radius:0;
                background:transparent;
                font-family:Arial, sans-serif;
                font-size:14px;
                line-height:1.45;
            ">
                {body}
            </div>
            """,
            height=PANEL_COMPONENT_HEIGHT,
        )


def charts_placeholder_box() -> None:
    with st.container(border=True, height=PANEL_OUTER_HEIGHT):
        st.markdown("#### Metaphor Usage Over Time")
        st.caption("Weekly post volume by metaphor.")

        df = get_metaphor_time_series()

        if df.empty:
            st.info("No aggregate metaphor data available yet.")
            return

        fig = px.line(
            df,
            x="week_start",
            y="n_posts",
            color="metaphor_category",
            markers=False,
            labels={
                "week_start": "Week",
                "n_posts": "Posts",
                "metaphor_category": "Metaphor",
            },
        )

        fig.update_traces(
            line=dict(width=2),
            opacity=0.75,
            hovertemplate=(
                "<b>%{fullData.name}</b><br>"
                "Week: %{x|%Y-%m-%d}<br>"
                "Posts: %{y:,}<extra></extra>"
            ),
        )

        fig.update_layout(
            height=PANEL_CONTENT_HEIGHT,
            margin=dict(l=10, r=10, t=20, b=90),
            hovermode="closest",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.22,
                xanchor="center",
                x=0.5,
                title=None,
            ),
            xaxis_title=None,
            yaxis_title="Posts",
            plot_bgcolor="white",
            paper_bgcolor="white",
        )

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=True, zeroline=False)

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displayModeBar": False,
                "responsive": True,
            },
        )


# --------------------------------------------------
# PAGE
# --------------------------------------------------

def run_home_page() -> None:
    metrics = get_home_metrics()

    total_posts = int(metrics["total_posts"])
    total_excl_last_7_days = int(metrics["total_excl_last_7_days"])
    last_7_days_posts = int(metrics["last_7_days_posts"])
    prev_7_days_posts = int(metrics["prev_7_days_posts"])

    delta_total = pct_delta(total_posts, total_excl_last_7_days)
    delta_week = pct_delta(last_7_days_posts, prev_7_days_posts)

    metaphor_df = get_metaphor_cycle_metrics()
    granularity_df = get_granularity_cycle_metrics()

    left_col, right_col = st.columns([1.45, 1.55])

    with left_col:
        st.title("FrameScope Home")
        st.subheader("Welcome to the FrameScope Dashboard")

        latest_summary = get_latest_week_overall_summary()

        st.markdown("#### This Week's Summary")
        st.markdown(
            f"""
            <div style="
                border-left: 4px solid #2563EB;
                padding-left: 1rem;
                margin-top: 0.5rem;
                color: #374151;
                font-style: italic;
                text-align: justify;
                line-height: 1.55;
            ">
                {html.escape(latest_summary)}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right_col:
        top_row_left, top_row_right = st.columns(2)

        with top_row_left:
            st.metric(
                label="Total Posts",
                value=f"{total_posts:,}",
                delta=delta_total,
            )

        with top_row_right:
            st.metric(
                label="This Week",
                value=f"{last_7_days_posts:,}",
                delta=delta_week,
            )

        bottom_row_left, bottom_row_right = st.columns(2)

        with bottom_row_left:
            animated_metric_card(
                title="Metaphor",
                rows=metaphor_df,
                key="metaphor_metric",
                seconds_per_item=7,
            )

        with bottom_row_right:
            animated_metric_card(
                title="Granularity",
                rows=granularity_df,
                key="granularity_metric",
                seconds_per_item=7,
            )

    st.divider()

    method_col, charts_col = st.columns([1, 1])

    with method_col:
        methodology_box()

    with charts_col:
        charts_placeholder_box()

    page_button()


if __name__ == "__main__":
    run_home_page()