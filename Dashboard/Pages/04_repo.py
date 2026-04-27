from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine


@st.cache_resource
def get_engine():
    neon_url = st.secrets.get("NeonDb")

    if not neon_url:
        st.error("NeonDb secret not found. Add it in Streamlit Cloud → Settings → Secrets.")
        st.stop()
    if not neon_url:
        return None
    return create_engine(neon_url, pool_pre_ping=True)


@st.cache_data(ttl=120)
def get_neon_status() -> str:
    engine = get_engine()

    if engine is None:
        return "Neon database is not configured."

    try:
        df = pd.read_sql_query(
            """
            SELECT COUNT(*) AS n_rows
            FROM aggregate_weekly_metrics;
            """,
            engine,
        )
        return f"Connected · {int(df.loc[0, 'n_rows']):,} aggregate rows available"
    except Exception:
        return "Neon database connection unavailable."


@st.cache_data(ttl=300)
def load_aggregate_metrics_csv() -> bytes:
    engine = get_engine()

    if engine is None:
        return b""

    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM aggregate_weekly_metrics
            ORDER BY week_start, subreddit, metaphor_category, granularity, stance;
            """,
            engine,
        )

        return df.to_csv(index=False).encode("utf-8")

    except Exception:
        return b""


def contact_card(name: str, role: str, email: str) -> None:
    st.markdown(
        f"""
        <div style="
            padding: 1.1rem 1.2rem;
            border: 1px solid rgba(49, 51, 63, 0.14);
            border-radius: 14px;
            background: #FFFFFF;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
            min-height: 140px;
        ">
            <h4 style="margin-bottom: 0.25rem;">{name}</h4>
            <p style="margin: 0 0 0.6rem 0; color: #6B7280;">{role}</p>
            <a href="mailto:{email}" style="
                color: #2563EB;
                text-decoration: none;
                font-weight: 500;
            ">{email}</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


def resource_button(label: str, url: str) -> None:
    st.markdown(
        f"""
        <a href="{url}" target="_blank" style="text-decoration: none;">
            <div style="
                padding: 1rem 1.2rem;
                border-radius: 12px;
                background: #EEF2FF;
                color: #1D4ED8;
                font-weight: 600;
                text-align: center;
                border: 1px solid #C7D2FE;
                margin-bottom: 0.75rem;
            ">
                {label}
            </div>
        </a>
        """,
        unsafe_allow_html=True,
    )


def repo_navigation_buttons() -> None:
    left, middle, right = st.columns([1, 1.6, 1])

    with left:
        back_clicked = st.button(
            "← Go Back To Reports",
            use_container_width=True,
        )

    with right:
        home_clicked = st.button(
            "Go To Home →",
            use_container_width=True,
        )

    if back_clicked:
        st.switch_page("Pages/03_report.py")

    if home_clicked:
        st.switch_page("Pages/01_home.py")


def run_repo_page() -> None:
    st.title("FrameScope Repository")
    st.caption("Project links, contributors, and lab information.")

    st.markdown("---")

    st.markdown("### Project Resources")

    col1, col2, col3 = st.columns(3)

    with col1:
        resource_button(
            "GitHub Repository",
            "https://github.com/Sagnik-Chakravarty/FrameScope",
        )

    with col2:
        resource_button(
            "CATS Group Lab",
            "https://cats-group.github.io/",
        )

    with col3:
        csv_data = load_aggregate_metrics_csv()

        if csv_data != b"":
            button_html = """
            <div style="
                padding: 1rem 1.2rem;
                border-radius: 12px;
                background: #EEF2FF;
                color: #1D4ED8;
                font-weight: 600;
                text-align: center;
                border: 1px solid #C7D2FE;
                margin-bottom: 0.75rem;
            ">
                Download Aggregate Metrics
            </div>
            """

            st.markdown(button_html, unsafe_allow_html=True)

            st.download_button(
                label="Download",
                data=csv_data,
                file_name="framescope_aggregate_weekly_metrics.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with st.container(border=True):
        st.markdown("### Database Status")
        st.write(get_neon_status())

    st.markdown("---")

    st.markdown("### Project Team")

    card1, card2, card3 = st.columns(3)

    with card1:
        contact_card(
            name="Sagnik Chakravarty",
            role="Graduate Research Assistant",
            email="sagnikch@umd.edu",
        )

    with card2:
        contact_card(
            name="Julia Mendelsohn",
            role="Faculty Lead",
            email="juliame@umd.edu",
        )

    with card3:
        contact_card(
            name="Tanya Joshi",
            role="Research Collaborator",
            email="tjoshi1@umd.edu",
        )

    st.markdown("---")

    with st.container(border=True):
        st.markdown("### About FrameScope")
        st.markdown(
            """
            FrameScope is a dashboard and analysis pipeline for studying how people describe,
            frame, and evaluate artificial intelligence in public online discourse.
            """
        )

    st.markdown("---")

    repo_navigation_buttons()


if __name__ == "__main__":
    run_repo_page()