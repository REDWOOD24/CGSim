import time, json, math, sqlite3, os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from time import sleep
from vis_utils.time_controls import render_time_controls, is_live_mode, get_time_filter_sql, get_events_at_timestep, update_timestep_metric

st.set_page_config(layout="wide",
                   page_title="CGSim - Overview",
                   page_icon="📊")

# Check for pending navigation FIRST before anything else
# This ensures navigation happens even if page reruns
if 'pending_navigation' in st.session_state and st.session_state.pending_navigation:
    target_page = st.session_state.pending_navigation
    st.session_state.pending_navigation = None
    st.switch_page(target_page)

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
if "output_dir" not in st.session_state:
    st.session_state.output_dir = DEFAULT_CONFIG["output_dir"]
if "output_filename" not in st.session_state:
    st.session_state.output_filename = DEFAULT_CONFIG["output_filename"]
if "config_dir" not in st.session_state:
    st.session_state.config_dir = DEFAULT_CONFIG["config_dir"]
if "selected_config_file" not in st.session_state:
    st.session_state.selected_config_file = DEFAULT_CONFIG["selected_config_file"]

# Always recalculate derived paths from current session state values
st.session_state.output_db_path = os.path.join(st.session_state.output_dir, st.session_state.output_filename)
st.session_state.site_info_json_path = os.path.join(st.session_state.config_dir, st.session_state.selected_config_file)

output_db_path = st.session_state.output_db_path
site_info_json_path = st.session_state.site_info_json_path
global_sleep_time = .5 #sec

def get_assigned_job_count(df):
    """Count jobs that are assigned."""
    if df.empty or 'STATUS' not in df.columns:
        return 0
    return len(df[df['STATUS'] == 'assigned'])

def get_running_job_count(df):
    """Count jobs that are running."""
    if df.empty or 'STATUS' not in df.columns:
        return 0
    return len(df[df['STATUS'] == 'running'])

def get_finished_job_count(df):
    """Count jobs that are finished."""
    if df.empty or 'STATUS' not in df.columns:
        return 0
    return len(df[df['STATUS'] == 'finished'])

def site_info_json_to_df(site_info_json_path):
    """Convert site info JSON to a DataFrame for display."""
    with open(site_info_json_path, 'r') as file:
        sites_info = json.load(file)

    sites = []
    site_datas = []
    site_GFLOPSs = []

    for site_name, site_info in sites_info.items():
        # Extract GFLOPS from SITE_PROPERTIES
        site_props = site_info.get('SITE_PROPERTIES', {})
        site_GFLOPS = site_props.get('GFLOPS', 'N/A')

        # Build site data string from SITE_PROPERTIES
        site_data_parts = []
        for key, value in site_props.items():
            if key != 'GFLOPS':  # Already showing GFLOPS separately
                site_data_parts.append(f'{key}: {value}')
        site_data_string = ', '.join(site_data_parts)

        sites.append(site_name)
        site_datas.append(site_data_string)
        site_GFLOPSs.append(site_GFLOPS)

    return pd.DataFrame({'Site': sites, 'GFLOPS': site_GFLOPSs, 'Site Data': site_datas})

@st.cache_data
def get_sites_info_df(site_info_json_path):

    with open(site_info_json_path, 'r') as file:

        sites_info_df = pd.DataFrame(json.load(file))

    return sites_info_df

def get_all_sites_data(output_db_path, table='EVENTS', max_time=None):
    """Fetch job data from the EVENTS table and reconstruct job status per site.

    Args:
        output_db_path: Path to the SQLite database
        table: Table name (default 'EVENTS')
        max_time: Optional maximum timestep to filter by. If None, gets latest state.

    Returns a DataFrame with columns: JOB_ID, SITE, STATUS, HOST (CPU)
    Each row represents the latest status of a job up to max_time.
    """
    # Check if database file exists to avoid creating an empty one
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)
        # Check if table exists first
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        if cursor.fetchone() is None:
            conn.close()
            return pd.DataFrame()  # Table doesn't exist yet

        # Build time filter for the query
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""

        # Get the latest event for each job to determine current status
        # We focus on JobExecution and JobAllocation events to track job status
        query = f"""
            SELECT
                e.JOB_ID,
                e.STATUS,
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

        # Parse METADATA JSON to extract site and host info
        def parse_metadata(metadata_str):
            try:
                return json.loads(metadata_str)
            except:
                return {}

        df['metadata_parsed'] = df['METADATA'].apply(parse_metadata)
        df['SITE'] = df['metadata_parsed'].apply(lambda x: x.get('site', ''))
        df['CPU'] = df['metadata_parsed'].apply(lambda x: x.get('host', ''))

        # Drop the metadata columns
        df = df.drop(columns=['METADATA', 'metadata_parsed'])

        return df
    except Exception as e:
        return pd.DataFrame()  # Return empty DataFrame on error

def get_sites_status_summary(df, sites_info_df):
    """Calculate job status counts across all sites."""
    if df.empty:
        return {"assigned": 0, "running": 0, "finished": 0, "failed": 0}

    # Count jobs by status
    assigned_count = len(df[df['STATUS'] == 'assigned']) if 'STATUS' in df.columns else 0
    running_count = len(df[df['STATUS'] == 'running']) if 'STATUS' in df.columns else 0
    finished_count = len(df[df['STATUS'] == 'finished']) if 'STATUS' in df.columns else 0
    failed_count = len(df[df['STATUS'] == 'failed']) if 'STATUS' in df.columns else 0

    return {
        "assigned": assigned_count,
        "running": running_count,
        "finished": finished_count,
        "failed": failed_count
    }

def plotly_sites_status_pie(sites_status):
    """Create a pie chart showing job status distribution."""
    labels = [
        f"Assigned: {sites_status['assigned']}",
        f"Running: {sites_status['running']}",
        f"Finished: {sites_status['finished']}",
        f"Failed: {sites_status['failed']}"
    ]
    values = [
        sites_status['assigned'],
        sites_status['running'],
        sites_status['finished'],
        sites_status['failed']
    ]
    colors = ['orange', 'blue', 'green', 'red']

    # Filter out zero values to avoid empty slices
    filtered_data = [(l, v, c) for l, v, c in zip(labels, values, colors) if v > 0]
    if filtered_data:
        labels, values, colors = zip(*filtered_data)
    else:
        labels, values, colors = ['No Jobs'], [1], ['gray']

    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        marker=dict(colors=colors),
        rotation=90,
        textinfo='label+percent',
        insidetextorientation='radial'
    )])

    fig.update_layout(
        title=dict(
            text="<b>Job Status Overview</b>",
            x=0.5,
            xanchor="center"
        ),
        paper_bgcolor='black',
        font=dict(color='white'),
        height=350,
        margin=dict(t=80, b=40, l=20, r=20)
    )

    return fig

# Initialize session state for counters
if 'last_overview_hash' not in st.session_state:
    st.session_state.last_overview_hash = None

if 'overview_pie_counter' not in st.session_state:
    st.session_state.overview_pie_counter = 0

if 'overview_df_counter' not in st.session_state:
    st.session_state.overview_df_counter = 0

# Reload sites_info_df if config file has changed or not yet loaded
if 'sites_info_df' not in st.session_state or st.session_state.get('loaded_config_file') != site_info_json_path:
    st.session_state.sites_info_df = site_info_json_to_df(site_info_json_path)
    st.session_state.loaded_config_file = site_info_json_path

# Add title and controls
st.write("# CGSim Visualization")

# Add time navigation controls in the sidebar
selected_timestep = render_time_controls(output_db_path, sidebar=True)

# Create containers for the pie chart and status
pie_container = st.empty()
waiting_container = st.empty()
status_container = st.empty()

st.write("### Check a Row Selector box to Navigate to the Site Page")
df_container = st.empty()
total_sites_container = st.empty()

# Initial data fetch
with status_container:
    st.info("Reading output database...")

# Get initial data. Filter by selected timestep if not in live mode
df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)
if df_all.empty:
    with waiting_container:
        st.write(f"### Error fetching data from database:")
        st.write(f"### Please make sure the simulation is running and has created the database at {output_db_path}.")
        st.write("### Retrying...")
while df_all.empty:
    time.sleep(global_sleep_time)
    df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)

waiting_container.empty()

sites_df = st.session_state.sites_info_df.copy()
sites_df.insert(1, 'Assigned', sites_df['Site'].apply(lambda site: get_assigned_job_count(df_all[df_all['SITE'] == site])))
sites_df.insert(2, 'Running', sites_df['Site'].apply(lambda site: get_running_job_count(df_all[df_all['SITE'] == site])))
sites_df.insert(3, 'Finished', sites_df['Site'].apply(lambda site: get_finished_job_count(df_all[df_all['SITE'] == site])))
sites_df = sites_df.sort_values(by='Running', ascending=False)

sites_status = get_sites_status_summary(df_all, st.session_state.sites_info_df)
last_overview_hash = hash(str(sites_status))

# Display initial pie chart with counter-based key (to avoid streamlit similar key errors)
pie_container.plotly_chart(
    plotly_sites_status_pie(sites_status),
    width='stretch',
    key=f"overview_pie_{st.session_state.overview_pie_counter}"
)
st.session_state.overview_pie_counter += 1

if is_live_mode():
    status_container.success(f"Monitoring all sites (Live)")

# Check for pending selection from ANY previous dataframe widget BEFORE rendering new one
# This handles the case where user clicked, rerun was triggered, and counter incremented
for key in list(st.session_state.keys()):
    if key.startswith('sites_dataframe_'):
        widget_state = st.session_state[key]
        if hasattr(widget_state, 'selection') and widget_state.selection.rows:
            selected_row_index = list(widget_state.selection.rows)[0]
            st.session_state.site = sites_df.iloc[selected_row_index]['Site']
            # Clear the selection to prevent re-navigation
            del st.session_state[key]
            st.switch_page('pages/2_Site.py')
            st.stop()

# Display the sites dataframe with counter-based key
current_df_key = f"sites_dataframe_{st.session_state.overview_df_counter}"
with df_container:
    selection_result = st.dataframe(
        sites_df,
        on_select='rerun',
        selection_mode="single-row",
        hide_index=True,
        width='stretch',
        height = 1000,
        key=current_df_key
    )
st.session_state.overview_df_counter += 1

with total_sites_container:
    st.write(f"{len(st.session_state.sites_info_df)} Total Sites")

# Show events dataframe when paused
if not is_live_mode() and selected_timestep is not None:
    st.write("### Events at Current Timestep")
    events_df = get_events_at_timestep(output_db_path, selected_timestep)
    if not events_df.empty:
        st.dataframe(events_df, hide_index=True, width='stretch')
    else:
        st.info(f"No events found at timestep {selected_timestep:.4f}s")

# Live update loop - use a flag to allow clean exit for navigation
should_exit_loop = False
navigate_to_page = None

# Track the current timestep to detect changes from button callbacks
last_selected_timestep = st.session_state.get('selected_timestep')

while not should_exit_loop:
    time.sleep(global_sleep_time)

    # Update the timestep metric in the sidebar
    update_timestep_metric(output_db_path)

    # Check if timestep changed (from button callbacks) and rerun if so
    current_selected_timestep = st.session_state.get('selected_timestep')
    if current_selected_timestep != last_selected_timestep:
        st.rerun()

    # Only update if in live mode
    if is_live_mode():
        try:
            # Get new data (no time filter in live mode)
            new_df_all = get_all_sites_data(output_db_path)
            new_sites_status = get_sites_status_summary(new_df_all, st.session_state.sites_info_df)
            new_hash = hash(str(new_sites_status))

            # Only update if data changed
            if new_hash != last_overview_hash:
                # Update sites_df with new job counts
                new_sites_df = st.session_state.sites_info_df.copy()
                new_sites_df.insert(1, 'Assigned', new_sites_df['Site'].apply(lambda site: get_assigned_job_count(new_df_all[new_df_all['SITE'] == site])))
                new_sites_df.insert(2, 'Running', new_sites_df['Site'].apply(lambda site: get_running_job_count(new_df_all[new_df_all['SITE'] == site])))
                new_sites_df.insert(3, 'Finished', new_sites_df['Site'].apply(lambda site: get_finished_job_count(new_df_all[new_df_all['SITE'] == site])))
                new_sites_df = new_sites_df.sort_values(by='Running', ascending=False, ignore_index=True)

                # Update the pie chart with counter-based key
                pie_container.plotly_chart(
                    plotly_sites_status_pie(new_sites_status),
                    width='stretch',
                    key=f"overview_pie_{st.session_state.overview_pie_counter}"
                )
                st.session_state.overview_pie_counter += 1

                # Check for pending selection from ANY previous dataframe widget
                for key in list(st.session_state.keys()):
                    if key.startswith('sites_dataframe_'):
                        widget_state = st.session_state[key]
                        if hasattr(widget_state, 'selection') and widget_state.selection.rows:
                            selected_row_index = list(widget_state.selection.rows)[0]
                            st.session_state.site = new_sites_df.iloc[selected_row_index]['Site']
                            del st.session_state[key]
                            navigate_to_page = 'pages/2_Site.py'
                            should_exit_loop = True
                            break

                if should_exit_loop:
                    continue

                # Update the dataframe with counter-based key
                current_df_key = f"sites_dataframe_{st.session_state.overview_df_counter}"
                with df_container:
                    live_selection = st.dataframe(
                        new_sites_df,
                        on_select='rerun',
                        selection_mode="single-row",
                        hide_index=True,
                        width='stretch',
                        height = 1000,
                        key=current_df_key
                    )
                st.session_state.overview_df_counter += 1

                last_overview_hash = new_hash
                sites_df = new_sites_df

                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating overview: {e}")
            time.sleep(1)  # Wait a bit longer on error

# After loop exits (when navigation is needed), perform the navigation
if navigate_to_page:
    st.switch_page(navigate_to_page)