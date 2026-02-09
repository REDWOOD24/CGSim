import time, json, sqlite3, os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from vis_utils.time_controls import render_time_controls, is_live_mode, update_timestep_metric

st.set_page_config(layout="wide",
                   page_title="CGSim - Failed Jobs",
                   page_icon="📊")

# Load configuration from vis_config.json on first run
CONFIG_FILE_PATH = "vis_config.json"
DEFAULT_CONFIG = {
    "output_dir": "../output",
    "output_filename": "output.db",
    "config_dir": "../config-files",
    "selected_config_file": "site_info.json"
}

if "config_loaded" not in st.session_state:
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            with open(CONFIG_FILE_PATH, 'r') as f:
                saved_config = json.load(f)
        except (json.JSONDecodeError, IOError):
            saved_config = DEFAULT_CONFIG.copy()
    else:
        saved_config = DEFAULT_CONFIG.copy()

    st.session_state.output_dir = saved_config.get("output_dir", DEFAULT_CONFIG["output_dir"])
    st.session_state.output_filename = saved_config.get("output_filename", DEFAULT_CONFIG["output_filename"])
    st.session_state.config_dir = saved_config.get("config_dir", DEFAULT_CONFIG["config_dir"])
    st.session_state.selected_config_file = saved_config.get("selected_config_file", DEFAULT_CONFIG["selected_config_file"])
    st.session_state.output_db_path = os.path.join(st.session_state.output_dir, st.session_state.output_filename)
    st.session_state.site_info_json_path = os.path.join(st.session_state.config_dir, st.session_state.selected_config_file)
    st.session_state.config_loaded = True

# Fallback defaults if session state not set
if "output_db_path" not in st.session_state:
    st.session_state.output_db_path = os.path.join(DEFAULT_CONFIG["output_dir"], DEFAULT_CONFIG["output_filename"])

if "site_info_json_path" not in st.session_state:
    st.session_state.site_info_json_path = os.path.join(DEFAULT_CONFIG["config_dir"], DEFAULT_CONFIG["selected_config_file"])

output_db_path = st.session_state.output_db_path
site_info_json_path = st.session_state.site_info_json_path
global_sleep_time = 0.5  # sec


def get_all_sites_data(output_db_path, table='EVENTS', max_time=None):
    """Fetch job data from the EVENTS table and reconstruct job status.

    Args:
        output_db_path: Path to the SQLite database
        table: Table name (default 'EVENTS')
        max_time: Optional maximum timestep to filter by. If None, gets latest state.

    Returns a DataFrame with columns: JOB_ID, SITE, STATUS, CPU (host), etc.
    """
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        if cursor.fetchone() is None:
            conn.close()
            return pd.DataFrame()

        # Build time filter for the query
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""

        # Get the latest event for each job
        query = f"""
            SELECT
                e.JOB_ID,
                e.STATUS,
                e.EVENT,
                e.STATE,
                e.TIME,
                e.METADATA
            FROM EVENTS e
            INNER JOIN (
                SELECT JOB_ID, MAX(_ID) as max_id
                FROM EVENTS
                WHERE EVENT IN ('JobExecution', 'JobAllocation')
                {time_filter}
                GROUP BY JOB_ID
            ) latest ON e._ID = latest.max_id
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return pd.DataFrame()

        # Parse METADATA JSON
        def parse_metadata(metadata_str):
            try:
                return json.loads(metadata_str)
            except:
                return {}

        df['metadata_parsed'] = df['METADATA'].apply(parse_metadata)
        df['SITE'] = df['metadata_parsed'].apply(lambda x: x.get('site', ''))
        df['CPU'] = df['metadata_parsed'].apply(lambda x: x.get('host', ''))
        df['CORES'] = df['metadata_parsed'].apply(lambda x: x.get('cores', 0))
        df['FLOPS'] = df['metadata_parsed'].apply(lambda x: x.get('flops', 0))
        df['DURATION'] = df['metadata_parsed'].apply(lambda x: x.get('duration', 0))

        # Clean up
        df = df.drop(columns=['METADATA', 'metadata_parsed'])

        return df
    except Exception:
        return pd.DataFrame()

def get_failed_jobs(df_all):
    """Filter to get only failed jobs."""
    if df_all.empty or 'STATUS' not in df_all.columns:
        return pd.DataFrame()
    return df_all[df_all['STATUS'] == 'failed']

def get_failed_jobs_by_site(df_failed):
    """Get count of failed jobs per site."""
    if df_failed.empty or 'SITE' not in df_failed.columns:
        return {}
    return df_failed.groupby('SITE').size().to_dict()

def get_site_summary_df(failed_by_site):
    """Create a summary dataframe of failed jobs per site."""
    if not failed_by_site:
        return pd.DataFrame(columns=['Site', 'Failed Jobs'])
    df = pd.DataFrame(list(failed_by_site.items()), columns=['Site', 'Failed Jobs'])
    return df.sort_values(by='Failed Jobs', ascending=False).reset_index(drop=True)

def get_valid_sites(site_info_json_path):
    """Get list of valid site names from site_info.json."""
    try:
        with open(site_info_json_path, 'r') as f:
            site_info = json.load(f)
        return list(site_info.keys())
    except Exception:
        return []


def plotly_failed_jobs_pie(failed_by_site):
    """Create a pie chart showing failed jobs per site."""
    if not failed_by_site:
        labels = ['No Failed Jobs']
        values = [1]
        colors = ['gray']
    else:
        labels = [f"{site}: {count}" for site, count in failed_by_site.items()]
        values = list(failed_by_site.values())
        colors = [f'hsl({i * 360 // len(failed_by_site)}, 70%, 50%)' for i in range(len(failed_by_site))]

    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        marker=dict(colors=colors),
        rotation=90,
        textinfo='label+percent',
        insidetextorientation='radial'
    )])

    fig.update_layout(
        title=dict(text="<b>Failed Jobs by Site</b>", x=0.5, xanchor="center", font_size=25),
        paper_bgcolor='black',
        font=dict(color='white'),
        height=600,
        margin=dict(t=120, b=40, l=20, r=20)
    )
    return fig

# Initialize session state
if 'failed_jobs_chart_counter' not in st.session_state:
    st.session_state.failed_jobs_chart_counter = 0

# Page title
st.write("# Failed Jobs Overview")

# Sidebar controls - time navigation
selected_timestep = render_time_controls(output_db_path, sidebar=True)

if st.sidebar.button("Back to Overview"):
    st.switch_page('CGSim_Visualization.py')

# Load valid sites from site_info.json
valid_sites = get_valid_sites(site_info_json_path)

# Create containers
metrics_container = st.empty()
pie_container = st.empty()
st.write("### Failed Jobs by Site")
st.write("#### Check a Row Selector box to Navigate to the Site Page")
site_summary_container = st.empty()
warning_container = st.empty()
status_container = st.empty()
st.write("### All Failed Jobs")
df_container = st.empty()

# Initial data fetch (filtered by selected timestep if not in live mode)
df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)
while df_all.empty:
    status_container.info("Waiting for database...")
    time.sleep(global_sleep_time)
    df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)

df_failed = get_failed_jobs(df_all)
failed_by_site = get_failed_jobs_by_site(df_failed)
last_hash = hash(str(df_failed.to_dict()))

# Display metrics
metrics_cols = metrics_container.columns(2)
metrics_cols[0].metric("Total Failed Jobs", len(df_failed))
metrics_cols[1].metric("Sites with Failures", len(failed_by_site))

# Display pie chart
pie_container.plotly_chart(
    plotly_failed_jobs_pie(failed_by_site),
    width='stretch',
    key=f"failed_pie_{st.session_state.failed_jobs_chart_counter}"
)
st.session_state.failed_jobs_chart_counter += 1

# Check for pending selection from ANY previous dataframe widget BEFORE rendering new one
for key in list(st.session_state.keys()):
    if key.startswith('failed_df_'):
        widget_state = st.session_state[key]
        if hasattr(widget_state, 'selection') and widget_state.selection.rows:
            selected_row_index = list(widget_state.selection.rows)[0]
            site_summary_df = get_site_summary_df(failed_by_site)
            if not site_summary_df.empty:
                selected_site = site_summary_df.iloc[selected_row_index]['Site']
                if selected_site in valid_sites:
                    warning_container.empty()
                    st.session_state.site = selected_site
                    del st.session_state[key]
                    st.switch_page('pages/2_Site.py')
                    st.stop()

# Display site summary dataframe with selection
site_summary_df = get_site_summary_df(failed_by_site)
if not site_summary_df.empty:
    with site_summary_container:
        site_selection = st.dataframe(
            site_summary_df,
            on_select='rerun',
            selection_mode="single-row",
            hide_index=True,
            width='stretch',
            key=f"failed_df_{st.session_state.failed_jobs_df_counter}"
        )
    st.session_state.failed_jobs_df_counter += 1
    # Handle site selection - navigate to site page
    if site_selection['selection']['rows'] != []:
        selected_row_index = site_selection['selection']['rows'][0]
        selected_site = site_summary_df.iloc[selected_row_index]['Site']
        # Check if the selected site is configured in site_info.json
        if selected_site in valid_sites:
            warning_container.empty()
            st.session_state.site = selected_site
            st.switch_page('pages/2_Site.py')
            st.stop()  # Ensure no further code runs after navigation
        else:
            warning_container.warning(f"Site '{selected_site}' is not configured in site_info.json. Cannot navigate to site page.")
else:
    site_summary_container.info("No sites with failed jobs.")

# Display all failed jobs dataframe
if not df_failed.empty:
    df_container.dataframe(df_failed.sort_values(by='SITE'), hide_index=True, width='stretch', height=600)
else:
    df_container.info("No failed jobs found.")

# Status
if is_live_mode():
    status_container.success("Monitoring failed jobs (Live)")

# Show events dataframe when paused
if not is_live_mode() and selected_timestep is not None:
    import sqlite3 as sqlite3_events
    import os as os_events
    if os_events.path.exists(output_db_path):
        conn_events = sqlite3_events.connect(output_db_path)
        query_events = "SELECT _ID, EVENT, STATE, STATUS, JOB_ID, TIME, METADATA FROM EVENTS WHERE TIME = ?"
        events_df = pd.read_sql_query(query_events, conn_events, params=(selected_timestep,))
        conn_events.close()
        st.write("### Events at Current Timestep")
        if not events_df.empty:
            st.dataframe(events_df, hide_index=True, width='stretch')
        else:
            st.info(f"No events found at timestep {selected_timestep:.4f}s")


# Live update loop - use flag to allow clean exit for navigation
should_exit_loop = False
navigate_to_page = None

# Track the current timestep to detect changes from button callbacks
last_selected_timestep = st.session_state.get('selected_timestep')

while not should_exit_loop:
    time.sleep(global_sleep_time)

    # Check if timestep changed (from button callbacks) and rerun if so
    current_selected_timestep = st.session_state.get('selected_timestep')
    if current_selected_timestep != last_selected_timestep:
        st.rerun()

    # Only update if in live (playing) mode
    if is_live_mode():
        try:
            new_df_all = get_all_sites_data(output_db_path)
            new_df_failed = get_failed_jobs(new_df_all)
            new_hash = hash(str(new_df_failed.to_dict()))

            if new_hash != last_hash:
                new_failed_by_site = get_failed_jobs_by_site(new_df_failed)

                # Update metrics
                metrics_cols = metrics_container.columns(2)
                metrics_cols[0].metric("Total Failed Jobs", len(new_df_failed))
                metrics_cols[1].metric("Sites with Failures", len(new_failed_by_site))

                # Update pie chart
                pie_container.plotly_chart(
                    plotly_failed_jobs_pie(new_failed_by_site),
                    width='stretch',
                    key=f"failed_pie_{st.session_state.failed_jobs_chart_counter}"
                )
                st.session_state.failed_jobs_chart_counter += 1

                # Check for pending selection from ANY previous dataframe widget
                new_site_summary_df = get_site_summary_df(new_failed_by_site)
                for key in list(st.session_state.keys()):
                    if key.startswith('failed_df_'):
                        widget_state = st.session_state[key]
                        if hasattr(widget_state, 'selection') and widget_state.selection.rows:
                            selected_row_index = list(widget_state.selection.rows)[0]
                            if not new_site_summary_df.empty:
                                selected_site = new_site_summary_df.iloc[selected_row_index]['Site']
                                if selected_site in valid_sites:
                                    warning_container.empty()
                                    st.session_state.site = selected_site
                                    del st.session_state[key]
                                    navigate_to_page = 'pages/2_Site.py'
                                    should_exit_loop = True
                                    break

                if should_exit_loop:
                    continue

                # Update site summary dataframe
                if not new_site_summary_df.empty:
                    with site_summary_container:
                        live_site_selection = st.dataframe(
                            new_site_summary_df,
                            on_select='rerun',
                            selection_mode="single-row",
                            hide_index=True,
                            width='stretch',
                            key=f"failed_df_{st.session_state.failed_jobs_df_counter}"
                        )
                    st.session_state.failed_jobs_df_counter += 1
                else:
                    site_summary_container.info("No sites with failed jobs.")

                # Update all failed jobs dataframe
                if not new_df_failed.empty:
                    df_container.dataframe(
                        new_df_failed.sort_values(by='SITE'),
                        hide_index=True,
                        width='stretch',
                        height=600
                    )
                else:
                    df_container.info("No failed jobs found.")

                last_hash = new_hash
                df_failed = new_df_failed
                failed_by_site = new_failed_by_site

                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating: {e}")
            time.sleep(1)

# After loop exits (when navigation is needed), perform the navigation
if navigate_to_page:
    st.switch_page(navigate_to_page)
