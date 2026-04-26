import streamlit as st

APP_TITLE = "FrameScope Dashboard"
APP_ICON = "📊"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    # Set to 'collapsed' to hide the sidebar completely by default
    initial_sidebar_state="collapsed", 
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
