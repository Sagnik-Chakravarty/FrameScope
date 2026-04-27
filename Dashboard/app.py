import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

APP_TITLE = "FrameScope Dashboard"
APP_ICON = "📊"


@st.cache_resource
def get_engine():
    neon_url = st.secrets["NeonDb"]
    return create_engine(neon_url, pool_pre_ping=True)


@st.cache_data(ttl=120)
def get_dataset_last_updated() -> str:
    engine = get_engine()

    try:
        row = pd.read_sql_query(
            """
            SELECT MAX(created_at) AS last_updated
            FROM pipeline_runs
            WHERE source = 'reddit';
            """,
            engine,
        )

        if not row.empty and pd.notna(row.loc[0, "last_updated"]):
            return str(row.loc[0, "last_updated"])

        row = pd.read_sql_query(
            """
            SELECT MAX(week_end) AS latest_week
            FROM aggregate_weekly_metrics;
            """,
            engine,
        )

        if not row.empty and pd.notna(row.loc[0, "latest_week"]):
            return f"{row.loc[0, 'latest_week']} (latest aggregate week)"

    except Exception:
        return "unknown"

    return "unknown"


st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = {
    "Menu": [
        st.Page("Pages/01_home.py", title="Home", icon=":material/home:"),
        st.Page("Pages/02_reddit.py", title="Reddit", icon=":material/forum:"),
        st.Page("Pages/03_report.py", title="Report", icon=":material/description:"),
        st.Page("Pages/04_repo.py", title="Resources", icon=":material/code:"),
    ]
}

navigation = st.navigation(pages, position="top")
navigation.run()

st.divider()
st.caption(
    f"Disclaimer: Dashboard metrics reflect the latest available dataset snapshot. "
    f"Last updated: {get_dataset_last_updated()}."
)