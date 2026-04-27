import streamlit as st
import sqlite3
from pathlib import Path
from datetime import datetime

APP_TITLE = "FrameScope Dashboard"
APP_ICON = "📊"
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "database" / "framescope.db"


@st.cache_data(ttl=120)
def get_dataset_last_updated(db_path: Path = DB_PATH) -> str:
    if not db_path.exists():
        return "unknown (database file not found)"

    conn = sqlite3.connect(db_path)

    try:
        # Prefer explicit pipeline run metadata when available.
        row = conn.execute(
            """
            SELECT MAX(created_at)
            FROM pipeline_runs
            WHERE source = 'reddit';
            """
        ).fetchone()

        if row and row[0]:
            return str(row[0])

        # Fallback to latest aggregate week date.
        row = conn.execute(
            """
            SELECT MAX(week_end)
            FROM aggregate_weekly_metrics;
            """
        ).fetchone()

        if row and row[0]:
            return f"{row[0]} (latest aggregate week)"

    except sqlite3.Error:
        pass
    finally:
        conn.close()

    # Final fallback to file modification time.
    modified = datetime.fromtimestamp(db_path.stat().st_mtime)
    return modified.strftime("%Y-%m-%d %H:%M:%S") + " (database file modified)"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    # Keep sidebar visible by default.
    initial_sidebar_state="expanded",
)

# Use a dictionary to create a "Menu" dropdown at the top
pages = {
    "Menu": [
        st.Page("Pages/01_home.py", title="Home", icon=":material/home:"),
        st.Page("Pages/02_reddit.py", title="Reddit", icon=":material/forum:"),
        st.Page("Pages/03_report.py", title="Report", icon=":material/description:"),
        st.Page("Pages/04_repo.py", title="Repo", icon=":material/code:"),
    ]
}

# position="top" moves the menu from the sidebar to the header
navigation = st.navigation(pages, position="top")

navigation.run()

st.divider()
st.caption(
    f"Disclaimer: Dashboard metrics reflect the latest available dataset snapshot. "
    f"Last updated: {get_dataset_last_updated()}."
)
