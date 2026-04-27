import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


DB_PATH = Path("data/database/framescope.db")


# --------------------------------------------------
# DATA
# --------------------------------------------------

@st.cache_data(ttl=60)
def load_report_options(db_path: Path = DB_PATH) -> dict:
    conn = sqlite3.connect(db_path)

    weeks = pd.read_sql_query(
        """
        SELECT DISTINCT week_start, week_end
        FROM weekly_llm_summary
        ORDER BY week_start DESC;
        """,
        conn,
    )

    months = pd.read_sql_query(
        """
        SELECT DISTINCT month
        FROM monthly_llm_summary
        ORDER BY month DESC;
        """,
        conn,
    )

    years = pd.read_sql_query(
        """
        SELECT DISTINCT year
        FROM yearly_llm_summary
        ORDER BY year DESC;
        """,
        conn,
    )

    conn.close()

    return {
        "weeks": weeks,
        "months": months["month"].dropna().tolist() if not months.empty else [],
        "years": years["year"].dropna().tolist() if not years.empty else [],
    }


@st.cache_data(ttl=60)
def load_report_data(
    db_path: Path,
    period_type: str,
    selected_period: str,
) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)

    if period_type == "Weekly":
        query = """
        SELECT *
        FROM weekly_llm_summary
        WHERE week_start = ?
        ORDER BY scope, scope_value;
        """
    elif period_type == "Monthly":
        query = """
        SELECT *
        FROM monthly_llm_summary
        WHERE month = ?
        ORDER BY scope, scope_value;
        """
    else:
        query = """
        SELECT *
        FROM yearly_llm_summary
        WHERE year = ?
        ORDER BY scope, scope_value;
        """

    df = pd.read_sql_query(query, conn, params=(selected_period,))
    conn.close()

    return df


# --------------------------------------------------
# FILTERS
# --------------------------------------------------

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

        years = sorted(week_df["year"].dropna().unique(), reverse=True)

        selected_year = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

        week_df = week_df[week_df["year"] == selected_year]

        months = week_df[["month", "week_start"]].copy()
        months["month_num"] = months["week_start"].dt.month
        month_options = (
            months.drop_duplicates("month")
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
            f"{row.week_start.strftime('%d')} - {row.week_end.strftime('%d')}"
            for row in week_df.sort_values("week_start", ascending=False).itertuples()
        ]

        selected_week = st.sidebar.selectbox(
            "Week",
            options=week_options,
            index=0 if week_options else None,
        )

        selected_period = (
            selected_week.split(" - ")[0]
            if selected_week
            else None
        )

    elif period_type == "Monthly":
        months = pd.DataFrame({"month": options["months"]})
        months["date"] = pd.to_datetime(months["month"])
        months["year"] = months["date"].dt.year
        months["month_name"] = months["date"].dt.strftime("%B")
        months["month_num"] = months["date"].dt.month

        years = sorted(months["year"].dropna().unique(), reverse=True)

        selected_year = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

        months = months[months["year"] == selected_year]

        month_options = (
            months.sort_values("month_num", ascending=False)["month_name"]
            .tolist()
        )

        selected_month_name = st.sidebar.selectbox(
            "Month",
            options=month_options,
            index=0 if month_options else None,
        )

        selected_row = months[months["month_name"] == selected_month_name]

        selected_period = (
            selected_row.iloc[0]["month"]
            if not selected_row.empty
            else None
        )

    else:
        years = sorted(options["years"], reverse=True)

        selected_period = st.sidebar.selectbox(
            "Year",
            options=years,
            index=0 if years else None,
        )

    return {
        "period_type": period_type,
        "selected_period": selected_period,
    }

# --------------------------------------------------
# REPORT DISPLAY
# --------------------------------------------------

def report_window(df: pd.DataFrame, filters: dict) -> None:
    with st.container(border=True):
        st.markdown("### Report Window")

        if filters["selected_period"] is None:
            st.warning("No report period available.")
            return

        st.caption(
            f"{filters['period_type']} report: {filters['selected_period']}"
        )

        if df.empty:
            st.info("No report available for the selected period.")
            return

        st.info("Report content will render here.")

        with st.expander("Report data preview"):
            st.dataframe(df.head(50), use_container_width=True)


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
            "Go To Repo →",
            use_container_width=True,
        )

    if back_clicked:
        st.switch_page("Pages/02_reddit.py")

    if forward_clicked:
        st.switch_page("Pages/04_repo.py")


# --------------------------------------------------
# PAGE
# --------------------------------------------------

def run_report_page(db_path: Path = DB_PATH) -> None:
    st.title("FrameScope Reports")
    st.caption("Weekly, monthly, and yearly summaries of AI discourse.")

    if not db_path.exists():
        st.error(f"Database not found: `{db_path}`")
        st.stop()

    options = load_report_options(db_path)
    filters = render_report_filters(options)

    if filters["selected_period"] is None:
        st.warning("No reports found.")
        report_navigation_buttons()
        return

    df = load_report_data(
        db_path=db_path,
        period_type=filters["period_type"],
        selected_period=filters["selected_period"],
    )

    report_window(df, filters)

    st.divider()

    report_navigation_buttons()


if __name__ == "__main__":
    run_report_page()