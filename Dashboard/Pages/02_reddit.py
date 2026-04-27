import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


# --------------------------------------------------
# DATABASE
# --------------------------------------------------

@st.cache_resource
def get_engine():
    neon_url = st.secrets["NeonDb"]
    return create_engine(neon_url, pool_pre_ping=True)


def table_exists(table_name: str) -> bool:
    engine = get_engine()

    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        ) AS exists;
        """
    )

    with engine.connect() as conn:
        return bool(conn.execute(query, {"table_name": table_name}).scalar())


# --------------------------------------------------
# DATA
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_shift_summaries() -> pd.DataFrame:
    if not table_exists("volume_shift_summary"):
        return pd.DataFrame()

    engine = get_engine()

    df = pd.read_sql_query(
        """
        SELECT
            period_type,
            period_start,
            scope,
            scope_value,
            shift_summary,
            key_transitions,
            volume_change,
            stance_shift,
            metaphor_shift
        FROM volume_shift_summary;
        """,
        engine,
    )

    if not df.empty:
        df["period_start"] = pd.to_datetime(df["period_start"], errors="coerce")

    return df


@st.cache_data(ttl=60)
def load_filter_options() -> dict:
    if not table_exists("aggregate_weekly_metrics"):
        today = pd.Timestamp.today().date()
        return {
            "subreddits": [],
            "metaphors": [],
            "granularities": [],
            "stances": [],
            "min_date": today,
            "max_date": today,
        }

    engine = get_engine()

    subreddits = pd.read_sql_query(
        """
        SELECT DISTINCT subreddit
        FROM aggregate_weekly_metrics
        WHERE subreddit IS NOT NULL
          AND TRIM(subreddit) != ''
        ORDER BY subreddit;
        """,
        engine,
    )["subreddit"].tolist()

    metaphors = pd.read_sql_query(
        """
        SELECT DISTINCT metaphor_category
        FROM aggregate_weekly_metrics
        WHERE metaphor_category IS NOT NULL
          AND TRIM(metaphor_category) != ''
        ORDER BY metaphor_category;
        """,
        engine,
    )["metaphor_category"].tolist()

    granularities = pd.read_sql_query(
        """
        SELECT DISTINCT granularity
        FROM aggregate_weekly_metrics
        WHERE granularity IS NOT NULL
          AND TRIM(granularity) != ''
        ORDER BY granularity;
        """,
        engine,
    )["granularity"].tolist()

    stances = pd.read_sql_query(
        """
        SELECT DISTINCT stance
        FROM aggregate_weekly_metrics
        WHERE stance IS NOT NULL
          AND TRIM(stance) != ''
        ORDER BY stance;
        """,
        engine,
    )["stance"].tolist()

    dates = pd.read_sql_query(
        """
        SELECT
            MIN(week_start) AS min_date,
            MAX(week_start) AS max_date
        FROM aggregate_weekly_metrics;
        """,
        engine,
    )

    if (
        dates.empty
        or pd.isna(dates.loc[0, "min_date"])
        or pd.isna(dates.loc[0, "max_date"])
    ):
        min_date = pd.Timestamp.today().date()
        max_date = pd.Timestamp.today().date()
    else:
        min_date = pd.to_datetime(dates.loc[0, "min_date"]).date()
        max_date = pd.to_datetime(dates.loc[0, "max_date"]).date()

    return {
        "subreddits": subreddits,
        "metaphors": metaphors,
        "granularities": granularities,
        "stances": stances,
        "min_date": min_date,
        "max_date": max_date,
    }


@st.cache_data(ttl=60)
def load_data(
    subreddits: list[str],
    metaphors: list[str],
    granularities: list[str],
    stances: list[str],
    include_none: bool,
    date_range: tuple,
) -> pd.DataFrame:
    engine = get_engine()

    df = pd.read_sql_query(
        """
        SELECT
            week_start,
            week_end,
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance,
            n_sentences,
            n_items,
            avg_score
        FROM aggregate_weekly_metrics;
        """,
        engine,
    )

    if df.empty:
        return df

    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    df["week_end"] = pd.to_datetime(df["week_end"], errors="coerce")

    start_date, end_date = date_range
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    df = df[
        (df["week_start"] >= start_date)
        & (df["week_start"] <= end_date)
    ]

    if subreddits and "All" not in subreddits:
        df = df[df["subreddit"].isin(subreddits)]

    if metaphors and "All" not in metaphors:
        df = df[df["metaphor_category"].isin(metaphors)]

    if granularities and "All" not in granularities:
        df = df[df["granularity"].isin(granularities)]

    if stances and "All" not in stances:
        df = df[df["stance"].isin(stances)]

    if not include_none:
        df = df[df["metaphor_category"] != "None"]

    return df


# --------------------------------------------------
# FILTERS
# --------------------------------------------------

def render_filters(options: dict) -> dict:
    st.sidebar.header("Filters")

    chart_type = st.sidebar.selectbox(
        "Chart Type",
        options=["Line Chart", "Bar Graph", "Pie Chart"],
        index=0,
    )

    date_range = st.sidebar.slider(
        "Time Period",
        min_value=options["min_date"],
        max_value=options["max_date"],
        value=(options["min_date"], options["max_date"]),
        format="YYYY-MM-DD",
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Chart Settings")

    metric_options = {
        "Posts": "n_items",
        "AI Sentences": "n_sentences",
        "Average Score": "avg_score",
    }

    y_axis_label = st.sidebar.selectbox(
        "Y-Axis",
        options=list(metric_options.keys()),
        index=0,
    )

    group_by_label = st.sidebar.selectbox(
        "Group / Color By",
        options=["Overall", "Metaphor", "Granularity", "Subreddit", "Stance"],
        index=0,
    )

    group_map = {
        "Overall": None,
        "Metaphor": "metaphor_category",
        "Granularity": "granularity",
        "Subreddit": "subreddit",
        "Stance": "stance",
    }

    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Filters")

    selected_subreddits = st.sidebar.multiselect(
        "Subreddit",
        options=["All"] + options["subreddits"],
        default=["All"],
    )

    selected_metaphors = st.sidebar.multiselect(
        "Metaphor",
        options=["All"] + options["metaphors"],
        default=["All"],
    )

    selected_granularities = st.sidebar.multiselect(
        "Granularity",
        options=["All"] + options["granularities"],
        default=["All"],
    )

    selected_stances = st.sidebar.multiselect(
        "Stance",
        options=["All"] + options["stances"],
        default=["All"],
    )

    include_none = st.sidebar.checkbox(
        "Include 'None' Metaphor",
        value=True,
    )

    return {
        "chart_type": chart_type,
        "date_range": date_range,
        "x_axis": "week_start",
        "x_axis_label": "Week",
        "y_axis": metric_options[y_axis_label],
        "y_axis_label": y_axis_label,
        "group_by_label": group_by_label,
        "group_by": group_map[group_by_label],
        "subreddits": selected_subreddits,
        "metaphors": selected_metaphors,
        "granularities": selected_granularities,
        "stances": selected_stances,
        "include_none": include_none,
    }


# --------------------------------------------------
# CHART HELPERS
# --------------------------------------------------

def aggregate_for_chart(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    group_by = filters["group_by"]
    y_col = filters["y_axis"]

    group_cols = ["week_start"] if group_by is None else ["week_start", group_by]

    if y_col == "avg_score":
        return df.groupby(group_cols, as_index=False)[y_col].mean()

    return df.groupby(group_cols, as_index=False)[y_col].sum()


def attach_shift_tooltips(chart_df: pd.DataFrame) -> pd.DataFrame:
    shift_df = load_shift_summaries()

    tooltip_cols = [
        "shift_summary",
        "key_transitions",
        "volume_change",
        "stance_shift",
        "metaphor_shift",
    ]

    for col in tooltip_cols:
        chart_df[col] = ""

    if shift_df.empty:
        return chart_df

    shift_df = shift_df[
        (shift_df["period_type"] == "month")
        & (shift_df["scope"] == "overall")
    ].copy()

    if shift_df.empty:
        return chart_df

    chart_df["period_start"] = (
        pd.to_datetime(chart_df["week_start"])
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    chart_df = chart_df.drop(columns=tooltip_cols, errors="ignore")

    chart_df = chart_df.merge(
        shift_df[
            [
                "period_start",
                "shift_summary",
                "key_transitions",
                "volume_change",
                "stance_shift",
                "metaphor_shift",
            ]
        ],
        on="period_start",
        how="left",
    )

    for col in tooltip_cols:
        chart_df[col] = chart_df[col].fillna("")

    return chart_df


def build_line_chart(chart_df: pd.DataFrame, filters: dict):
    y_col = filters["y_axis"]
    group_by = filters["group_by"]

    chart_df = attach_shift_tooltips(chart_df)

    tooltip_cols = [
        "shift_summary",
        "key_transitions",
        "volume_change",
        "stance_shift",
        "metaphor_shift",
    ]

    labels = {
        "week_start": "Week",
        y_col: filters["y_axis_label"],
    }

    if group_by:
        labels[group_by] = filters["group_by_label"]

    fig = px.line(
        chart_df,
        x="week_start",
        y=y_col,
        color=group_by,
        markers=True,
        custom_data=tooltip_cols,
        labels=labels,
    )

    fig.update_traces(
        line=dict(width=2.5),
        marker=dict(size=6),
        hovertemplate=(
            "<b>%{fullData.name}</b><br>"
            "Week: %{x|%Y-%m-%d}<br>"
            f"{filters['y_axis_label']}: " + "%{y:,}<br><br>"
            "<b>Shift Summary</b><br>%{customdata[0]}"
            "<extra></extra>"
        ),
    )

    return fig


def build_bar_chart(chart_df: pd.DataFrame, filters: dict):
    y_col = filters["y_axis"]
    group_by = filters["group_by"]

    if group_by is None:
        bar_df = chart_df
        x_col = "week_start"
        color_col = None
        x_label = "Week"
    else:
        bar_df = chart_df.groupby(group_by, as_index=False)[y_col].sum()
        x_col = group_by
        color_col = group_by
        x_label = filters["group_by_label"]

    fig = px.bar(
        bar_df,
        x=x_col,
        y=y_col,
        color=color_col,
        labels={
            x_col: x_label,
            y_col: filters["y_axis_label"],
        },
    )

    return fig


def build_pie_chart(chart_df: pd.DataFrame, filters: dict):
    y_col = filters["y_axis"]
    group_by = filters["group_by"]

    if group_by is None:
        pie_df = pd.DataFrame(
            {
                "category": ["Overall"],
                y_col: [chart_df[y_col].sum()],
            }
        )
        names_col = "category"
        label = "Overall"
    else:
        pie_df = chart_df.groupby(group_by, as_index=False)[y_col].sum()
        names_col = group_by
        label = filters["group_by_label"]

    fig = px.pie(
        pie_df,
        names=names_col,
        values=y_col,
        labels={
            names_col: label,
            y_col: filters["y_axis_label"],
        },
    )

    return fig


# --------------------------------------------------
# DASHBOARD WINDOW
# --------------------------------------------------

def dashboard_window(df: pd.DataFrame, filters: dict) -> None:
    with st.container(border=True):
        st.markdown("### Analysis Window")

        if df.empty:
            st.warning("No data available for the selected filters.")
            return

        chart_df = aggregate_for_chart(df, filters)

        if chart_df.empty:
            st.warning("No chart data available after aggregation.")
            return

        if filters["chart_type"] == "Line Chart":
            fig = build_line_chart(chart_df, filters)
        elif filters["chart_type"] == "Bar Graph":
            fig = build_bar_chart(chart_df, filters)
        else:
            fig = build_pie_chart(chart_df, filters)

        fig.update_layout(
            height=560,
            margin=dict(l=10, r=10, t=30, b=70),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend_title_text=filters["group_by_label"],
        )

        if filters["chart_type"] != "Pie Chart":
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

        with st.expander("Filtered data preview"):
            st.dataframe(df.head(50), use_container_width=True)


# --------------------------------------------------
# NAVIGATION
# --------------------------------------------------

def dashboard_navigation_buttons() -> None:
    left, middle, right = st.columns([1, 1.6, 1])

    with left:
        back_clicked = st.button(
            "← Go To Home",
            use_container_width=True,
        )

    with right:
        forward_clicked = st.button(
            "Go To Report →",
            use_container_width=True,
        )

    if back_clicked:
        st.switch_page("Pages/01_home.py")

    if forward_clicked:
        st.switch_page("Pages/03_report.py")


# --------------------------------------------------
# PAGE
# --------------------------------------------------

def run_dashboard_page() -> None:
    st.title("Reddit AI Discourse Dashboard")

    options = load_filter_options()
    filters = render_filters(options)

    df = load_data(
        subreddits=filters["subreddits"],
        metaphors=filters["metaphors"],
        granularities=filters["granularities"],
        stances=filters["stances"],
        include_none=filters["include_none"],
        date_range=filters["date_range"],
    )

    dashboard_window(df, filters)

    st.divider()

    dashboard_navigation_buttons()


if __name__ == "__main__":
    run_dashboard_page()