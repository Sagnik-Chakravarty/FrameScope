import pandas as pd
import streamlit as st
from sqlalchemy import create_engine


# --------------------------------------------------
# DATABASE
# --------------------------------------------------

@st.cache_resource
def get_engine():
    neon_url = st.secrets["NeonDb"]
    return create_engine(neon_url, pool_pre_ping=True)


# --------------------------------------------------
# DATA
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_report_options() -> dict:
    engine = get_engine()

    weekly_reports = pd.read_sql_query(
        """
        SELECT *
        FROM weekly_llm_summary
        ORDER BY week_start DESC, scope, scope_value;
        """,
        engine,
    )

    monthly_reports = pd.read_sql_query(
        """
        SELECT *
        FROM monthly_llm_summary
        ORDER BY month DESC, scope, scope_value;
        """,
        engine,
    )

    yearly_reports = pd.read_sql_query(
        """
        SELECT *
        FROM yearly_llm_summary
        ORDER BY year DESC, scope, scope_value;
        """,
        engine,
    )

    weeks = weekly_reports[["week_start", "week_end"]].drop_duplicates()
    months = monthly_reports[["month"]].drop_duplicates()
    years = yearly_reports[["year"]].drop_duplicates()

    return {
        "weeks": weeks,
        "months_df": months,
        "years_df": years,
        "weekly_reports": weekly_reports,
        "monthly_reports": monthly_reports,
        "yearly_reports": yearly_reports,
    }


# --------------------------------------------------
# FILTERS
# --------------------------------------------------

def default_index(options: list, preferred: str) -> int:
    return options.index(preferred) if preferred in options else 0


def render_report_filters(options: dict) -> dict:
    st.sidebar.header("Report Filters")

    period_type = st.sidebar.selectbox(
        "Report Type",
        options=["Weekly", "Monthly", "Yearly"],
        index=0,
    )

    selected_period = None

    if period_type == "Weekly":
        week_df = options["weeks"].copy()

        week_df["week_start"] = pd.to_datetime(week_df["week_start"])
        week_df["week_end"] = pd.to_datetime(week_df["week_end"])
        week_df["year"] = week_df["week_start"].dt.year
        week_df["month"] = week_df["week_start"].dt.strftime("%B")
        week_df["month_num"] = week_df["week_start"].dt.month

        years = sorted(week_df["year"].dropna().unique(), reverse=True)

        selected_year = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

        week_df = week_df[week_df["year"] == selected_year]

        month_options = (
            week_df[["month", "month_num"]]
            .drop_duplicates()
            .sort_values("month_num", ascending=False)["month"]
            .tolist()
        )

        selected_month = st.sidebar.selectbox(
            "Month",
            options=month_options,
            index=0 if month_options else None,
        )

        week_df = week_df[week_df["month"] == selected_month]

        week_options = [
            f"{row.week_start.strftime('%m-%d')} - {row.week_end.strftime('%m-%d')}"
            for row in week_df.sort_values("week_start", ascending=False).itertuples()
        ]

        selected_week = st.sidebar.selectbox(
            "Week",
            options=week_options,
            index=0 if week_options else None,
        )

        if selected_week:
            selected_row = week_df[
                week_df.apply(
                    lambda r: (
                        f"{r['week_start'].strftime('%m-%d')} - "
                        f"{r['week_end'].strftime('%m-%d')}"
                    )
                    == selected_week,
                    axis=1,
                )
            ]

            if not selected_row.empty:
                selected_period = selected_row.iloc[0]["week_start"].strftime(
                    "%Y-%m-%d"
                )

        report_df = options["weekly_reports"].copy()

        if selected_period:
            report_df = report_df[report_df["week_start"] == selected_period]

    elif period_type == "Monthly":
        month_df = options["months_df"].copy()

        month_df["date"] = pd.to_datetime(month_df["month"])
        month_df["year"] = month_df["date"].dt.year
        month_df["month_name"] = month_df["date"].dt.strftime("%B")
        month_df["month_num"] = month_df["date"].dt.month

        years = sorted(month_df["year"].dropna().unique(), reverse=True)

        selected_year = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

        month_df = month_df[month_df["year"] == selected_year]

        month_options = (
            month_df.sort_values("month_num", ascending=False)["month_name"].tolist()
        )

        selected_month = st.sidebar.selectbox(
            "Month",
            options=month_options,
            index=0 if month_options else None,
        )

        selected_row = month_df[month_df["month_name"] == selected_month]

        if not selected_row.empty:
            selected_period = selected_row.iloc[0]["month"]

        report_df = options["monthly_reports"].copy()

        if selected_period:
            report_df = report_df[report_df["month"] == selected_period]

    else:
        year_df = options["years_df"].copy()
        years = sorted(year_df["year"].dropna().unique(), reverse=True)

        selected_period = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

        report_df = options["yearly_reports"].copy()

        if selected_period:
            report_df = report_df[report_df["year"] == selected_period]

    st.sidebar.markdown("---")
    st.sidebar.subheader("Summary Filters")

    selected_scope = "overall"
    selected_subreddit = "All"
    selected_granularity = "All"
    selected_stance = "All"

    if not report_df.empty:
        if "scope" in report_df.columns:
            scope_options = sorted(report_df["scope"].dropna().unique().tolist())

            selected_scope = st.sidebar.selectbox(
                "Scope",
                options=scope_options,
                index=default_index(scope_options, "overall"),
            )

            report_df = report_df[report_df["scope"] == selected_scope]

        if selected_scope == "subreddit" and "scope_value" in report_df.columns:
            subreddit_options = ["All"] + sorted(
                report_df["scope_value"].dropna().unique().tolist()
            )

            selected_subreddit = st.sidebar.selectbox(
                "Subreddit",
                options=subreddit_options,
                index=0,
            )

            if selected_subreddit != "All":
                report_df = report_df[report_df["scope_value"] == selected_subreddit]

        if "dominant_granularity" in report_df.columns:
            granularity_options = ["All"] + sorted(
                report_df["dominant_granularity"].dropna().unique().tolist()
            )

            selected_granularity = st.sidebar.selectbox(
                "Granularity",
                options=granularity_options,
                index=0,
            )

            if selected_granularity != "All":
                report_df = report_df[
                    report_df["dominant_granularity"] == selected_granularity
                ]

        if "dominant_stance" in report_df.columns:
            stance_options = ["All"] + sorted(
                report_df["dominant_stance"].dropna().unique().tolist()
            )

            selected_stance = st.sidebar.selectbox(
                "Stance",
                options=stance_options,
                index=0,
            )

            if selected_stance != "All":
                report_df = report_df[
                    report_df["dominant_stance"] == selected_stance
                ]

    return {
        "period_type": period_type,
        "selected_period": selected_period,
        "scope": selected_scope,
        "subreddit": selected_subreddit,
        "dominant_granularity": selected_granularity,
        "dominant_stance": selected_stance,
        "filtered_report_df": report_df,
    }


# --------------------------------------------------
# REPORT DISPLAY
# --------------------------------------------------

def clean_value(value, fallback: str = "—") -> str:
    if pd.isna(value) or not str(value).strip():
        return fallback
    return str(value)


def render_dominant_metaphors(metaphor_value) -> None:
    if pd.isna(metaphor_value) or not str(metaphor_value).strip():
        return

    metaphors = [
        m.strip()
        for m in str(metaphor_value).replace(";", ",").split(",")
        if m.strip()
    ]

    if not metaphors:
        return

    st.markdown("#### Dominant Metaphor")

    for metaphor in metaphors:
        st.markdown(f"- **{metaphor}**")


def report_window(df: pd.DataFrame, filters: dict) -> None:
    with st.container(border=True):
        st.markdown("## Report")

        if filters["selected_period"] is None:
            st.warning("No report period available.")
            return

        st.caption(f"{filters['period_type']} report · {filters['selected_period']}")

        if df.empty:
            st.info("No report available for the selected filters.")
            return

        for i, row in df.iterrows():
            scope = clean_value(row.get("scope", "overall"))
            scope_value = clean_value(row.get("scope_value", "overall"))
            granularity = clean_value(
                row.get("granularity", row.get("dominant_granularity", "—"))
            )
            stance = clean_value(row.get("dominant_stance", "—"))
            metaphor = row.get("dominant_metaphors", "")
            evidence_count = row.get("evidence_count", 0)

            title = f"{scope.replace('_', ' ').title()} · {scope_value}"

            st.markdown(f"### {title}")

            meta_cols = st.columns(3)

            with meta_cols[0]:
                st.metric("Granularity", granularity)

            with meta_cols[1]:
                st.metric("Dominant Stance", stance)

            with meta_cols[2]:
                try:
                    st.metric("Evidence Count", int(evidence_count))
                except Exception:
                    st.metric("Evidence Count", 0)

            summary_text = row.get("summary_text", "")
            likely_drivers = row.get("likely_drivers", "")
            notable_shift = row.get("notable_shift", "")

            if pd.notna(summary_text) and str(summary_text).strip():
                st.markdown("#### Summary")
                st.markdown(str(summary_text))

            if pd.notna(likely_drivers) and str(likely_drivers).strip():
                st.markdown("#### Likely Drivers")
                st.markdown(str(likely_drivers))

            render_dominant_metaphors(metaphor)

            if pd.notna(notable_shift) and str(notable_shift).strip():
                st.markdown("#### Notable Shift")
                st.markdown(str(notable_shift))

            with st.expander("Metadata"):
                st.write(
                    {
                        "model_name": row.get("model_name", None),
                        "generated_at": row.get("generated_at", None),
                        "scope": row.get("scope", None),
                        "scope_value": row.get("scope_value", None),
                    }
                )

            if i != df.index[-1]:
                st.markdown("---")


# --------------------------------------------------
# NAVIGATION
# --------------------------------------------------

def report_navigation_buttons() -> None:
    left, middle, right = st.columns([1, 1.6, 1])

    with left:
        back_clicked = st.button(
            "← Go Back To Dashboard",
            use_container_width=True,
        )

    with right:
        forward_clicked = st.button(
            "Go To Resources →",
            use_container_width=True,
        )

    if back_clicked:
        st.switch_page("Pages/02_reddit.py")

    if forward_clicked:
        st.switch_page("Pages/04_repo.py")


# --------------------------------------------------
# PAGE
# --------------------------------------------------

def run_report_page() -> None:
    st.title("FrameScope Reports")
    st.caption("Weekly, monthly, and yearly summaries of AI discourse.")

    options = load_report_options()
    filters = render_report_filters(options)

    if filters["selected_period"] is None:
        st.warning("No reports found.")
        report_navigation_buttons()
        return

    df = filters["filtered_report_df"]

    report_window(df, filters)

    st.divider()

    report_navigation_buttons()


if __name__ == "__main__":
    run_report_page()