from __future__ import annotations

import streamlit as st
from sqlalchemy import create_engine


@st.cache_resource
def get_engine():
    neon_url = st.secrets["NeonDb"]
    return create_engine(neon_url, pool_pre_ping=True)