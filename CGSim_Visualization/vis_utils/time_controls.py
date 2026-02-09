"""
Shared time navigation controls for the REDWOOD visualization app.

This module provides:
- Session state initialization for time navigation
- UI controls (play/pause, step back/forward, slider, dropdown)
- Functions to get available timesteps from the database
- Functions to filter data queries by selected timestep
"""

import streamlit as st
import sqlite3
import os
import pandas as pd
from typing import List, Optional, Tuple


def get_available_timesteps(db_path: str) -> List[float]:
    """Get all unique timesteps from the EVENTS table, sorted ascending."""
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT TIME FROM EVENTS ORDER BY TIME")
        timesteps = [row[0] for row in cursor.fetchall()]
        conn.close()
        return timesteps
    except sqlite3.Error:
        return []


def get_max_timestep(db_path: str) -> Optional[float]:
    """Get the maximum timestep from the EVENTS table."""
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(TIME) FROM EVENTS")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    except sqlite3.Error:
        return None


def init_time_state():
    """Initialize session state variables for time navigation."""
    if 'time_playing' not in st.session_state:
        st.session_state.time_playing = True  # Start in "playing" (live) mode
    if 'selected_timestep' not in st.session_state:
        st.session_state.selected_timestep = None  # None means "latest"
    if 'timestep_index' not in st.session_state:
        st.session_state.timestep_index = -1  # -1 means "latest"


def render_time_controls(db_path: str, sidebar: bool = True) -> Optional[float]:
    """
    Render time navigation controls and return the selected timestep.
    
    Args:
        db_path: Path to the SQLite database
        sidebar: If True, render in sidebar; otherwise render in main area
    
    Returns:
        The selected timestep (float), or None if in "live" mode (latest)
    """
    init_time_state()
    
    # Get available timesteps
    timesteps = get_available_timesteps(db_path)
    
    if not timesteps:
        container = st.sidebar if sidebar else st
        container.warning("No timesteps available in database")
        return None
    
    container = st.sidebar if sidebar else st

    container.write("## Time Controls")

    # Store timesteps in session state so callbacks can access them
    st.session_state._timesteps_cache = timesteps

    # Define callback functions for buttons
    def on_pause():
        ts = st.session_state._timesteps_cache
        st.session_state.time_playing = False
        st.session_state.selected_timestep = ts[-1]
        st.session_state.timestep_index = len(ts) - 1

    def on_play():
        st.session_state.time_playing = True
        st.session_state.selected_timestep = None
        st.session_state.timestep_index = -1

    def on_step_back():
        ts = st.session_state._timesteps_cache
        st.session_state.timestep_index -= 1
        st.session_state.selected_timestep = ts[st.session_state.timestep_index]

    def on_step_fwd():
        ts = st.session_state._timesteps_cache
        st.session_state.timestep_index += 1
        st.session_state.selected_timestep = ts[st.session_state.timestep_index]

    # Play/Pause and Step buttons in a row
    col1, col2, col3 = container.columns([1, 1, 1])

    with col1:
        if st.session_state.time_playing:
            st.button("⏸️ Pause", key="time_pause_btn", use_container_width=True, on_click=on_pause)
        else:
            st.button("▶️ Play", key="time_play_btn", use_container_width=True, on_click=on_play)

    with col2:
        step_back_disabled = st.session_state.time_playing or st.session_state.timestep_index <= 0
        st.button("⏮️ Back", key="time_back_btn", disabled=step_back_disabled, use_container_width=True, on_click=on_step_back)

    with col3:
        step_fwd_disabled = st.session_state.time_playing or st.session_state.timestep_index >= len(timesteps) - 1
        st.button("⏭️ Fwd", key="time_fwd_btn", disabled=step_fwd_disabled, use_container_width=True, on_click=on_step_fwd)
    
    # Create an empty placeholder for the timestep metric that can be updated live
    if 'timestep_metric_container' not in st.session_state:
        st.session_state.timestep_metric_container = container.empty()
    else:
        # Re-create the container on each page load to ensure it's in the right place
        st.session_state.timestep_metric_container = container.empty()

    # Initial render of the metric
    if st.session_state.time_playing:
        current_ts = timesteps[-1] if timesteps else 0.0
    else:
        current_ts = st.session_state.selected_timestep if st.session_state.selected_timestep is not None else 0.0
    st.session_state.timestep_metric_container.metric("Current Timestep", f"{current_ts:.4f}s")

    # Slider for scanning timesteps (only enabled when paused)
    if not st.session_state.time_playing and len(timesteps) > 1:
        container.write("### Timestep Slider")
        new_index = container.slider(
            "Select timestep",
            min_value=0,
            max_value=len(timesteps) - 1,
            value=st.session_state.timestep_index,
            format=f"Step %d",
            key="time_slider",
            label_visibility="collapsed"
        )
        if new_index != st.session_state.timestep_index:
            st.session_state.timestep_index = new_index
            st.session_state.selected_timestep = timesteps[new_index]
            st.rerun()
        
        # Dropdown for specific timestep selection
        container.write("### Select Timestep")
        timestep_options = {f"{i}: {t:.4f}s": i for i, t in enumerate(timesteps)}
        current_label = f"{st.session_state.timestep_index}: {timesteps[st.session_state.timestep_index]:.4f}s"
        
        selected_label = container.selectbox(
            "Choose timestep",
            options=list(timestep_options.keys()),
            index=st.session_state.timestep_index,
            key="time_dropdown",
            label_visibility="collapsed"
        )
        
        selected_idx = timestep_options[selected_label]
        if selected_idx != st.session_state.timestep_index:
            st.session_state.timestep_index = selected_idx
            st.session_state.selected_timestep = timesteps[selected_idx]
            st.rerun()
    
    # Show current status (only show LIVE indicator, paused state is shown by timestep metric)
    if st.session_state.time_playing:
        container.success("🔴 LIVE - Following latest updates")

    return st.session_state.selected_timestep


def update_timestep_metric(db_path: str):
    """
    Update the timestep metric in the sidebar with the latest timestep.
    Call this in the while loop to keep the metric updated during live mode.
    """
    if 'timestep_metric_container' not in st.session_state:
        return

    if st.session_state.get('time_playing', True):
        # In live mode, get the latest timestep from the database
        timesteps = get_available_timesteps(db_path)
        current_ts = timesteps[-1] if timesteps else 0.0
    else:
        # In paused mode, show the selected timestep
        current_ts = st.session_state.get('selected_timestep', 0.0) or 0.0

    st.session_state.timestep_metric_container.metric("Current Timestep", f"{current_ts:.4f}s")


def get_time_filter_sql(selected_timestep: Optional[float]) -> str:
    """
    Generate SQL WHERE clause fragment for filtering by timestep.

    Args:
        selected_timestep: The selected timestep, or None for latest

    Returns:
        SQL fragment like "AND TIME <= 0.5" or empty string if None
    """
    if selected_timestep is None:
        return ""
    return f"AND TIME <= {selected_timestep}"


def is_live_mode() -> bool:
    """Check if we're in live (playing) mode."""
    init_time_state()
    return st.session_state.time_playing


def get_current_timestep() -> Optional[float]:
    """Get the currently selected timestep, or None if in live mode."""
    init_time_state()
    return st.session_state.selected_timestep


def get_events_at_timestep(db_path: str, timestep: float) -> pd.DataFrame:
    """Get all events that occurred at a specific timestep."""
    import sqlite3

    if not os.path.exists(db_path):
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT _ID, EVENT, STATE, STATUS, JOB_ID, TIME, METADATA FROM EVENTS WHERE TIME = ?"
        df = pd.read_sql_query(query, conn, params=(timestep,))
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_timesteps_for_site(db_path: str, site: str) -> List[float]:
    """Get all unique timesteps that have events for a specific site."""
    import sqlite3

    if not os.path.exists(db_path):
        return []

    try:
        conn = sqlite3.connect(db_path)
        # Query events where METADATA contains the site name
        query = "SELECT DISTINCT TIME FROM EVENTS WHERE METADATA LIKE ? ORDER BY TIME"
        cursor = conn.cursor()
        cursor.execute(query, (f'%"{site}"%',))
        timesteps = [row[0] for row in cursor.fetchall()]
        conn.close()
        return timesteps
    except Exception:
        return []


def get_timesteps_for_cpu(db_path: str, site: str, cpu_num: int) -> List[float]:
    """Get all unique timesteps that have events for a specific CPU."""
    import sqlite3

    if not os.path.exists(db_path):
        return []

    try:
        conn = sqlite3.connect(db_path)
        # Query events where METADATA contains the cpu host name pattern
        cpu_pattern = f'%{site}%cpu-{cpu_num}%'
        query = "SELECT DISTINCT TIME FROM EVENTS WHERE METADATA LIKE ? ORDER BY TIME"
        cursor = conn.cursor()
        cursor.execute(query, (cpu_pattern,))
        timesteps = [row[0] for row in cursor.fetchall()]
        conn.close()
        return timesteps
    except Exception:
        return []


def render_site_step_controls(db_path: str, site: str, container=None):
    """Render site-specific step forward/back buttons."""
    if container is None:
        container = st.sidebar

    init_time_state()

    # Only show when paused
    if st.session_state.time_playing:
        return

    site_timesteps = get_timesteps_for_site(db_path, site)
    if not site_timesteps:
        return

    current_timestep = st.session_state.selected_timestep
    if current_timestep is None:
        return

    container.write("### Site Time Navigation")
    col1, col2 = container.columns(2)

    # Find previous site timestep
    prev_site_timesteps = [t for t in site_timesteps if t < current_timestep]
    prev_site_timestep = prev_site_timesteps[-1] if prev_site_timesteps else None

    # Find next site timestep
    next_site_timesteps = [t for t in site_timesteps if t > current_timestep]
    next_site_timestep = next_site_timesteps[0] if next_site_timesteps else None

    all_timesteps = get_available_timesteps(db_path)

    # Store values in session state for callbacks
    st.session_state._site_prev_timestep = prev_site_timestep
    st.session_state._site_next_timestep = next_site_timestep
    st.session_state._all_timesteps = all_timesteps

    def on_site_back():
        ts = st.session_state._site_prev_timestep
        all_ts = st.session_state._all_timesteps
        st.session_state.selected_timestep = ts
        st.session_state.timestep_index = all_ts.index(ts)

    def on_site_fwd():
        ts = st.session_state._site_next_timestep
        all_ts = st.session_state._all_timesteps
        st.session_state.selected_timestep = ts
        st.session_state.timestep_index = all_ts.index(ts)

    with col1:
        st.button("⏮️ Site Back", key="site_back_btn", disabled=prev_site_timestep is None, use_container_width=True, on_click=on_site_back)

    with col2:
        st.button("⏭️ Site Fwd", key="site_fwd_btn", disabled=next_site_timestep is None, use_container_width=True, on_click=on_site_fwd)


def render_cpu_step_controls(db_path: str, site: str, cpu_num: int, container=None):
    """Render CPU-specific step forward/back buttons."""
    if container is None:
        container = st.sidebar

    init_time_state()

    # Only show when paused
    if st.session_state.time_playing:
        return

    cpu_timesteps = get_timesteps_for_cpu(db_path, site, cpu_num)
    if not cpu_timesteps:
        return

    current_timestep = st.session_state.selected_timestep
    if current_timestep is None:
        return

    container.write("### CPU Time Navigation")
    col1, col2 = container.columns(2)

    # Find previous CPU timestep
    prev_cpu_timesteps = [t for t in cpu_timesteps if t < current_timestep]
    prev_cpu_timestep = prev_cpu_timesteps[-1] if prev_cpu_timesteps else None

    # Find next CPU timestep
    next_cpu_timesteps = [t for t in cpu_timesteps if t > current_timestep]
    next_cpu_timestep = next_cpu_timesteps[0] if next_cpu_timesteps else None

    all_timesteps = get_available_timesteps(db_path)

    # Store values in session state for callbacks
    st.session_state._cpu_prev_timestep = prev_cpu_timestep
    st.session_state._cpu_next_timestep = next_cpu_timestep
    st.session_state._all_timesteps = all_timesteps

    def on_cpu_back():
        ts = st.session_state._cpu_prev_timestep
        all_ts = st.session_state._all_timesteps
        st.session_state.selected_timestep = ts
        st.session_state.timestep_index = all_ts.index(ts)

    def on_cpu_fwd():
        ts = st.session_state._cpu_next_timestep
        all_ts = st.session_state._all_timesteps
        st.session_state.selected_timestep = ts
        st.session_state.timestep_index = all_ts.index(ts)

    with col1:
        st.button("⏮️ CPU Back", key="cpu_back_btn", disabled=prev_cpu_timestep is None, use_container_width=True, on_click=on_cpu_back)

    with col2:
        st.button("⏭️ CPU Fwd", key="cpu_fwd_btn", disabled=next_cpu_timestep is None, use_container_width=True, on_click=on_cpu_fwd)
