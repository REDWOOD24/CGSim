import time, json, sqlite3, os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from vis_utils.time_controls import render_time_controls, is_live_mode, get_events_at_timestep, render_cpu_step_controls, update_timestep_metric

st.set_page_config(layout="wide",
                   page_title="CGSim - CPU",
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
if "output_db_path" not in st.session_state:
    st.session_state.output_db_path = os.path.join(DEFAULT_CONFIG["output_dir"], DEFAULT_CONFIG["output_filename"])

if "site_info_json_path" not in st.session_state:
    st.session_state.site_info_json_path = os.path.join(DEFAULT_CONFIG["config_dir"], DEFAULT_CONFIG["selected_config_file"])

output_db_path = st.session_state.output_db_path
site_info_json_path = st.session_state.site_info_json_path
global_sleep_time = 0.5  # sec

@st.cache_data
def get_sites_info_df(site_info_json_path):
    """Load site info from JSON file."""
    with open(site_info_json_path, 'r') as file:
        sites_info_df = pd.DataFrame(json.load(file))
    return sites_info_df

def get_site_cpus(sites_info_df, site):
    """Get total CPU count from the CPUInfo array in the new format."""
    try:
        cpu_info_list = sites_info_df[site]['CPUInfo']
        # Sum up all the 'units' from each CPU type
        total_cpus = sum(cpu['units'] for cpu in cpu_info_list)
        return total_cpus
    except (KeyError, TypeError):
        return 0

# Load site info
sites_info_df = get_sites_info_df(site_info_json_path)
unique_sites = sites_info_df.copy().columns.tolist()

# Default values if not set in session state
default_site = unique_sites[0] if unique_sites else "AGLT2_site_0"
default_cpu = 0

if 'site' not in st.session_state:
    st.session_state.site = default_site

if 'cpu' not in st.session_state:
    st.session_state.cpu = default_cpu

if 'cpu_chart_counter' not in st.session_state:
    st.session_state.cpu_chart_counter = 0

site = st.session_state.site
cpu_num = st.session_state.cpu
# Build the full CPU host name (format: SITE_cpu-N)
cpu_host_name = f"{site}_cpu-{cpu_num}"

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

def get_cpu_data(df_all, site, cpu_host_name):
    """Filter data for a specific CPU at a specific site."""
    if df_all.empty:
        return pd.DataFrame()
    # Match jobs by the full CPU host name
    return df_all[df_all['CPU'] == cpu_host_name]

def plotly_cpu_pie_chart(df_cpu):
    """Create a pie chart showing job status distribution for the CPU."""
    if df_cpu.empty:
        labels = ['No Jobs']
        values = [1]
        colors = ['gray']
    else:
        assigned_count = df_cpu['STATUS'].value_counts().get('assigned', 0)
        running_count = df_cpu['STATUS'].value_counts().get('running', 0)
        finished_count = df_cpu['STATUS'].value_counts().get('finished', 0)
        failed_count = df_cpu['STATUS'].value_counts().get('failed', 0)
        other_count = sum(count for status, count in df_cpu['STATUS'].value_counts().items() 
                        if status not in ['assigned', 'running', 'finished', 'failed'])
        labels = [f'Assigned Jobs: {assigned_count}', 
                  f'Running Jobs: {running_count}', 
                  f'Finished Jobs: {finished_count}',
                  f'Failed Jobs: {failed_count}',
                  f'Other: {other_count}']
        values = [assigned_count, running_count, finished_count, failed_count, other_count]
        colors = ['orange', 'blue', 'green', 'red', 'orange']
    
    fig = go.Figure(data=[go.Pie(labels=labels, 
                                 values=values, 
                                 marker=dict(colors=colors), 
                                 rotation=90)])
    fig.update_layout(
        title=dict(
            text="<b>Job Status Distribution</b>",
            x=0.5,
            xanchor="center",
            font_size=25
        ),
        paper_bgcolor='black',
        font=dict(color='white'),
        height=350,
        margin=dict(t=120, b=40, l=20, r=20)
    )
    return fig

# Page header
st.write("# CGSim Visualization")
st.write(f"### Site: {site} | CPU: cpu-{cpu_num}")

# Sidebar navigation
st.sidebar.write("## Navigation")
nav_col1, nav_col2 = st.sidebar.columns(2)

with nav_col1:
    if st.button("Back to Overview", use_container_width=True):
        st.switch_page('CGSim_Visualization.py')

with nav_col2:
    if st.button("Back to Site", use_container_width=True):
        st.switch_page('pages/2_Site.py')

# Site selection dropdown
st.sidebar.write("## Site Selection")

def on_site_change():
    new_site = st.session_state.cpu_page_site_select
    if new_site != st.session_state.site:
        # Get CPU count for new site
        new_site_cpus = get_site_cpus(sites_info_df, new_site)
        # If current CPU number exceeds new site's CPU count, reset to 0
        if st.session_state.cpu >= new_site_cpus:
            st.session_state.cpu = 0
            st.session_state.cpu_number_input = 0
        st.session_state.site = new_site

# Get current site index for default selection
current_site_index = unique_sites.index(site) if site in unique_sites else 0

st.sidebar.selectbox(
    'Select a Site:',
    unique_sites,
    index=current_site_index,
    key="cpu_page_site_select",
    on_change=on_site_change
)

# CPU number input - use session state key to maintain value across reruns
st.sidebar.write("## CPU Selection")
if 'cpu_number_input' not in st.session_state:
    st.session_state.cpu_number_input = cpu_num

# Get max CPU number for current site
site_cpus = get_site_cpus(sites_info_df, site)

def on_cpu_number_change():
    st.session_state.cpu = st.session_state.cpu_number_input

new_cpu = st.sidebar.number_input(
    "CPU Number",
    min_value=0,
    max_value=max(0, site_cpus - 1),  # Limit to valid CPU range
    key="cpu_number_input",
    on_change=on_cpu_number_change
)

# Time navigation controls
selected_timestep = render_time_controls(output_db_path, sidebar=True)

# Add CPU-specific step controls (only shown when paused)
render_cpu_step_controls(output_db_path, site, cpu_num)

# Create containers
pie_container = st.empty()
metrics_container = st.empty()
df_container = st.empty()
status_container = st.empty()

# Initial data fetch
with status_container:
    st.info("Reading output database...")

df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)
while df_all.empty:
    time.sleep(global_sleep_time)
    df_all = get_all_sites_data(output_db_path, max_time=selected_timestep)

df_cpu = get_cpu_data(df_all, site, cpu_host_name)
last_df_hash = hash(str(df_cpu.to_dict()))

# Display metrics
total_jobs = len(df_cpu)
active_jobs = len(df_cpu[df_cpu['STATUS'].isin(['running', 'assigned'])]) if not df_cpu.empty else 0
finished_jobs = len(df_cpu[df_cpu['STATUS'] == 'finished']) if not df_cpu.empty else 0
failed_jobs = len(df_cpu[df_cpu['STATUS'] == 'failed']) if not df_cpu.empty else 0

metrics_cols = metrics_container.columns(4)
metrics_cols[0].metric("Total Jobs", total_jobs)
metrics_cols[1].metric("Active Jobs", active_jobs)
metrics_cols[2].metric("Finished Jobs", finished_jobs)
metrics_cols[3].metric("Failed Jobs", failed_jobs)

# Display pie chart
pie_container.plotly_chart(
    plotly_cpu_pie_chart(df_cpu),
    use_container_width=True,
    key=f"cpu_pie_{st.session_state.cpu_chart_counter}"
)
st.session_state.cpu_chart_counter += 1

# Display dataframe
df_container.write("### Job History")
if not df_cpu.empty:
    df_container.dataframe(df_cpu.sort_values(by='TIME', ascending=False), hide_index=True, use_container_width=True)
else:
    df_container.info("No jobs found for this CPU.")

if is_live_mode():
    status_container.success(f"Monitoring cpu-{cpu_num} at {site} (Live)")

# Show events dataframe when paused
if not is_live_mode() and selected_timestep is not None:
    st.write("### Events at Current Timestep")
    filter_cpu_events = st.checkbox(f"Show only events for cpu-{cpu_num}", value=True, key="filter_cpu_events")
    events_df = get_events_at_timestep(output_db_path, selected_timestep)
    if not events_df.empty:
        if filter_cpu_events:
            # Filter to show only events for the current CPU (host name format: SITE_cpu-N)
            events_df = events_df[events_df['METADATA'].str.contains(f'"{cpu_host_name}"', na=False)]
        if not events_df.empty:
            st.dataframe(events_df, hide_index=True, use_container_width=True)
        else:
            st.info(f"No events found for cpu-{cpu_num} at timestep {selected_timestep:.4f}s")
    else:
        st.info(f"No events found at timestep {selected_timestep:.4f}s")

# Live update loop
while True:
    time.sleep(global_sleep_time)

    # Update the timestep metric in the sidebar
    update_timestep_metric(output_db_path)

    # Only update if in live (playing) mode
    if is_live_mode():
        try:
            new_df_all = get_all_sites_data(output_db_path)
            new_df_cpu = get_cpu_data(new_df_all, site, cpu_host_name)
            # Sort by TIME if available, otherwise skip sorting
            if not new_df_cpu.empty and 'TIME' in new_df_cpu.columns:
                new_df_cpu = new_df_cpu.sort_values(by='TIME', ascending=False)
            new_hash = hash(str(new_df_cpu.to_dict()))

            if new_hash != last_df_hash:
                # Update metrics
                new_total = len(new_df_cpu)
                new_active = len(new_df_cpu[new_df_cpu['STATUS'].isin(['running', 'assigned'])]) if not new_df_cpu.empty else 0
                new_finished = len(new_df_cpu[new_df_cpu['STATUS'] == 'finished']) if not new_df_cpu.empty else 0
                new_failed = len(new_df_cpu[new_df_cpu['STATUS'] == 'failed']) if not new_df_cpu.empty else 0

                metrics_cols = metrics_container.columns(4)
                metrics_cols[0].metric("Total Jobs", new_total)
                metrics_cols[1].metric("Active Jobs", new_active)
                metrics_cols[2].metric("Finished Jobs", new_finished)
                metrics_cols[3].metric("Failed Jobs", new_failed)

                # Update pie chart
                pie_container.plotly_chart(
                    plotly_cpu_pie_chart(new_df_cpu),
                    use_container_width=True,
                    key=f"cpu_pie_{st.session_state.cpu_chart_counter}"
                )
                st.session_state.cpu_chart_counter += 1

                # Update dataframe
                df_container.write("### Job History")
                if not new_df_cpu.empty:
                    df_container.dataframe(new_df_cpu, hide_index=True, use_container_width=True)
                else:
                    df_container.info("No jobs found for this CPU.")

                last_df_hash = new_hash
                df_cpu = new_df_cpu

                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating: {e}")
            time.sleep(1)
