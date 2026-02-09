import time, json, math, sqlite3, os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from vis_utils.time_controls import render_time_controls, is_live_mode, get_events_at_timestep, render_site_step_controls, update_timestep_metric

st.set_page_config(layout="wide",
                   page_title="CGSim - Site",
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
global_sleep_time = 0.5 #sec

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

@st.cache_data(ttl=global_sleep_time)
def get_site_data(output_db_path, site, table='EVENTS', max_time=None):
    """Fetch job data for a specific site from the EVENTS table.

    Args:
        output_db_path: Path to the SQLite database
        site: Site name to filter by
        table: Table name (default 'EVENTS')
        max_time: Optional maximum timestep to filter by. If None, gets latest state.

    Returns a DataFrame with job information reconstructed from events.
    """
    # Check if database file exists to avoid creating an empty one
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)

        # Build time filter for the query
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""

        # Get the latest event for each job that matches this site
        # We need to filter by site in the METADATA JSON
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
        df['SPEED'] = df['metadata_parsed'].apply(lambda x: x.get('speed', 0))
        df['DURATION'] = df['metadata_parsed'].apply(lambda x: x.get('duration', 0))
        df['SITE_CPU_UTIL'] = df['metadata_parsed'].apply(lambda x: x.get('site_cpu_util', 0))

        # Filter by site
        df = df[df['SITE'] == site]

        # Clean up
        df = df.drop(columns=['METADATA', 'metadata_parsed'])

        return df
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        time.sleep(global_sleep_time)
        return pd.DataFrame()  # Return empty DataFrame on error


@st.cache_data
def get_sites_info_df(site_info_json_path):

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
    except (KeyError, TypeError) as e:
        # Fallback to old GFLOPS-based calculation if CPUInfo is not available
        try:
            site_GFLOPS = sites_info_df[site]['SITE_PROPERTIES']['GFLOPS'][0]
            site_cores = 32
            site_GFLOPS_p_core = 500
            return round(int(site_GFLOPS) / (site_cores * site_GFLOPS_p_core))
        except:
            return 1  # Default to 1 if all else fails

def get_active_job_count(df):
    """Count jobs that are running or assigned."""
    if df.empty or 'STATUS' not in df.columns:
        return 0
    return len(df[df['STATUS'].isin(['running', 'assigned'])])

def get_busy_cpu_count(df, site_cpus):
    """Count CPUs with jobs that are assigned or running, and return percentage."""
    if df.empty or 'STATUS' not in df.columns or 'CPU #' not in df.columns:
        return 0, 0.0
    # Get unique CPUs that have assigned or running jobs
    busy_cpus = df[df['STATUS'].isin(['running', 'assigned'])]['CPU #'].nunique()
    percentage = (busy_cpus / site_cpus * 100) if site_cpus > 0 else 0.0
    return busy_cpus, percentage

def plotly_block_chart(df_site, site, site_cpus):
    # Dynamically adjust box size and column count based on CPU count
    if site_cpus <= 100:
        box_size = 30
        num_cols = 10
    elif site_cpus <= 500:
        box_size = 20
        num_cols = 20
    elif site_cpus <= 1000:
        box_size = 12
        num_cols = 40
    elif site_cpus <= 5000:
        box_size = 10
        num_cols = 70
    else:
        box_size = 12
        num_cols = 88
    
    min_cpu = 0
    max_cpu = site_cpus

    all_cpus = pd.DataFrame({'CPU #': list(range(int(min_cpu), int(max_cpu) + 1))})

    # If there are multiple jobs per CPU, keep only the one with the "most active" status
    # Priority: running > assigned > finished > failed > idle
    # This ensures the chart shows the current state of each CPU
    if not df_site.empty and 'CPU #' in df_site.columns:
        # Sort by CPU # and status priority (running/assigned first, finished last)
        status_priority = {'running': 0, 'assigned': 1, 'finished': 2, 'failed': 3}
        df_site_sorted = df_site.copy()
        df_site_sorted['status_priority'] = df_site_sorted['STATUS'].map(status_priority).fillna(4)
        df_site_sorted = df_site_sorted.sort_values(['CPU #', 'status_priority'])
        # Keep first (most active) job per CPU
        df_site_deduped = df_site_sorted.drop_duplicates(subset=['CPU #'], keep='first')
        df_site_deduped = df_site_deduped.drop(columns=['status_priority'])
    else:
        df_site_deduped = df_site

    df_merged = all_cpus.merge(df_site_deduped, on='CPU #', how='left')
    
    num_rows = round(math.sqrt(site_cpus))
    
    # Calculate aspect ratio to maintain square boxes
    aspect_ratio = num_cols / num_rows if num_rows > 0 else 1
    
    df_merged['index'] = df_merged['CPU #'] - min_cpu
    df_merged['col'] = df_merged['index'] % num_cols
    df_merged['row'] = df_merged['index'] // num_cols

    def build_hover(row):
        if pd.isnull(row.get('JOB_ID')):
            return f"CPU {int(row['CPU #'])}<br>Status: idle"
        else:
            # Format duration if available
            duration = row.get('DURATION', 0)
            duration_str = f"{duration:.2f}s" if duration else "N/A"

            # Format FLOPS
            flops = row.get('FLOPS', 0)
            flops_str = f"{flops:,.0f}" if flops else "N/A"

            hover_text = (
                f"CPU #: {int(row['CPU #'])}<br>"
                "Most Recent Job:<br>"
                f"Job ID: {row['JOB_ID']}<br>"
                f"Site: {row.get('SITE', 'N/A')}<br>"
                f"Status: {row.get('STATUS', 'N/A')}<br>"
                f"Cores: {row.get('CORES', 'N/A')}<br>"
                f"FLOPS: {flops_str}<br>"
                f"Duration: {duration_str}<br>"
                f"CPU Util: {row.get('SITE_CPU_UTIL', 0):.4%}"
            )
            return hover_text
        
    df_merged['hover'] = df_merged.apply(build_hover, axis=1)

    def get_color(status):
        if pd.isnull(status):
            return 'lightgrey'  # Idle CPU.
        elif status == 'finished':
            return 'green'
        elif status == 'running':
            return 'blue'
        elif status == 'assigned':
            return 'grey'
        elif status == 'failed':
            return 'red'
        else:
            return 'red'
        
    df_merged['color'] = df_merged['STATUS'].apply(get_color)

    fig = go.Figure(data=go.Scatter(
                                    x=df_merged['col'],
                                    y=df_merged['row'],
                                    mode='markers',
                                    marker=dict(
                                        symbol='square',
                                        size=box_size,
                                        color=df_merged['color'],
                                        line=dict(color='black', width=1)
                                    ),
                                    text=df_merged['hover'],
                                    hoverinfo='text',
                                    showlegend=False,
                                    customdata=df_merged['CPU #'].values
    ))

    # Calculate plot dimensions to maintain square boxes
    plot_width = 1700  # Base width

    fig.update_layout(
                    autosize=True,
                    xaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        range=[-1, num_cols]  # Set fixed range for x-axis
                    ),
                    yaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        autorange='reversed',  # So that row 0 appears at the top
                        range=[num_rows, -1]  # Set fixed range for y-axis
                    ),
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    hoverlabel=dict(font_size=40,  # make hover text bigger
                                    font_family="Arial"),
                    margin=dict(autoexpand=False, l=0, r=0, t=0, b=0),
                    width=plot_width,
    )

    return fig, num_rows

def plotly_pie_chart(df_site, site):
    assigned_count = df_site['STATUS'].value_counts().get('assigned', 0)
    running_count = df_site['STATUS'].value_counts().get('running', 0)
    finished_count = df_site['STATUS'].value_counts().get('finished', 0)
    other_count = sum(count for status, count in df_site['STATUS'].value_counts().items()
                    if status not in ['assigned', 'running', 'finished'])
    labels = [f'Assigned Jobs: {assigned_count}',
              f'Running Jobs: {running_count}',
              f'Finished Jobs: {finished_count}',
              f'Other: {other_count}']
    values = [assigned_count, running_count, finished_count, other_count]
    colors = ['orange', 'blue', 'green', 'red']
    # Return pie trace with legendgroup for separate legend
    return go.Pie(labels=labels,
                  values=values,
                  marker=dict(colors=colors),
                  rotation=90,
                  legendgroup="job_status",
                  legend="legend2",
                  hovertemplate="%{label}<br>" +
                  "Percent: %{percent}" +
                  "<extra></extra>")

def plotly_cpu_util_pie_chart(df_site, site_cpus):
    """Create a pie chart showing average CPU utilization across all CPUs at the site."""
    # Calculate average CPU utilization from the dataframe
    if not df_site.empty and 'SITE_CPU_UTIL' in df_site.columns:
        # Get average utilization - the SITE_CPU_UTIL is already a ratio (0-1)
        avg_util = df_site['SITE_CPU_UTIL'].mean()
        if pd.isna(avg_util):
            avg_util = 0.0
    else:
        avg_util = 0.0

    # Convert to percentage
    util_percent = avg_util * 100
    idle_percent = 100 - util_percent

    # Color based on utilization threshold (green if < 70%, red if >= 70%)
    util_color = 'green' if util_percent < 70 else 'red'

    labels = [f'Utilized: {util_percent:.1f}%', f'Idle: {idle_percent:.1f}%']
    values = [util_percent, idle_percent]
    colors = [util_color, 'grey']

    # Return pie trace with legendgroup for separate legend
    return go.Pie(labels=labels,
                  values=values,
                  marker=dict(colors=colors),
                  rotation=90,
                  hole=0.3,
                  legendgroup="cpu_util",
                  legend="legend",
                  hovertemplate="%{label}<br>" +
                  "<extra></extra>")

def plotly_site_subplot(df_site, site):
    cpu_util_trace = plotly_cpu_util_pie_chart(df_site, site_cpus)
    job_status_trace = plotly_pie_chart(df_site, site)
    fig_block, num_rows = plotly_block_chart(df_site, site, site_cpus)

    # Calculate appropriate subplot heights
    if site_cpus <= 100:
        row_heights = [0.25, 0.75]
    else:
        row_heights = [0.3, .85]

    # Create subplot with 2 rows: row 1 has 2 pie charts side by side, row 2 has block chart
    fig = make_subplots(rows=2,
                        cols=2,
                        specs=[[{'type': 'pie'}, {'type': 'pie'}],
                              [{'type': 'xy', 'colspan': 2}, None]],
                        row_heights=row_heights,
                        vertical_spacing=0.02,
                        horizontal_spacing=0.25)  # More space between pie charts for legends

    # Add CPU utilization pie chart (left - row 1, col 1)
    fig.add_trace(cpu_util_trace, row=1, col=1)

    # Add job status pie chart (right - row 1, col 2)
    fig.add_trace(job_status_trace, row=1, col=2)

    # Add block chart (row 2, spans both columns)
    for trace in fig_block.data:
        fig.add_trace(trace, row=2, col=1)

    # Calculate plot dimensions based on CPU count - increased width for legends
    if site_cpus <= 100:
        plot_width = 700  # Increased from 400 to fit legends
        plot_height = max(800, num_rows * 28)
        legend_size = 9
    elif site_cpus <= 500:
        plot_width = 800  # Increased from 500
        plot_height = max(800, num_rows * 28)
        legend_size = 11
    elif site_cpus <= 1000:
        plot_width = 900  # Increased from 600
        plot_height = max(800, num_rows * 28)
        legend_size = 13
    else:
        plot_width = 1100  # Increased from 850
        plot_height = max(800, num_rows * 28)
        legend_size = 14

    fig.update_layout(
                    title=dict(
                        text=f"<b>{site}</b>",
                        font_size=26,
                        y=0.99,
                        x=0.5,
                        xanchor="center",
                        yanchor="top"
                    ),
                    xaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False
                    ),
                    yaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        autorange='reversed'  # So that row 0 appears at the top
                    ),
                    plot_bgcolor='black',
                    paper_bgcolor='black',
                    margin=dict(l=0, r=0, t=50, b=0),
                    # Legend for CPU utilization pie chart (positioned to the right of left chart)
                    legend=dict(
                        title=dict(text="<b>CPU Utilization</b>", font=dict(size=legend_size)),
                        yanchor="top",
                        y=0.78,
                        xanchor="left",
                        x=0.31,
                        font_size=legend_size,
                        traceorder="normal",
                        bgcolor="rgba(0,0,0,0.5)"
                    ),
                    # Legend for job status pie chart (positioned to the right of right chart)
                    legend2=dict(
                        title=dict(text="<b>Job Status</b>", font=dict(size=legend_size)),
                        yanchor="top",
                        y=0.80,
                        xanchor="left",
                        x=0.50,
                        font_size=legend_size,
                        traceorder="normal",
                        bgcolor="rgba(0,0,0,0.5)"
                    ),
                    hoverlabel=dict(font_size=16,
                                    font_family="Arial"),
                    width=plot_width,
                    height=plot_height
    )

    # Add subplot titles as annotations
    fig.add_annotation(
        text="<b>Avg CPU Utilization</b>",
        xref="paper", yref="paper",
        x=0.33, y=1,
        showarrow=False,
        font=dict(size=14, color="white"),
        xanchor="center"
    )
    fig.add_annotation(
        text="<b>Job Status Distribution</b>",
        xref="paper", yref="paper",
        x=0.65, y=1,
        showarrow=False,
        font=dict(size=14, color="white"),
        xanchor="center"
    )

    return fig

# Initialize streamlit page and write title

site_info_json_df = site_info_json_to_df(site_info_json_path)
sites_info_df = get_sites_info_df(site_info_json_path)
unique_sites = sites_info_df.copy().columns.tolist()

default_site = site_info_json_df['Site'].iloc[0]

if 'site' not in st.session_state:

    st.session_state.site = default_site

site = st.session_state.site

st.write("# CGSim Visualization")
st.write(f"### {site}")

site = st.sidebar.selectbox('Select a Site:', unique_sites, unique_sites.index(site))


# Add this after the site selection in the sidebar
st.sidebar.write("## Navigation")
col1, col2 = st.sidebar.columns(2)

# Get the current site index
current_site_index = unique_sites.index(site)

# Add Previous Site button
with col1:
    if st.button("Previous Site"):
        # Get previous site (wrap around to the end if at the beginning)
        prev_index = (current_site_index - 1) % len(unique_sites)
        st.session_state.site = unique_sites[prev_index]
        st.rerun()

# Add Next Site button
with col2:
    if st.button("Next Site"):
        # Get next site (wrap around to the beginning if at the end)
        next_index = (current_site_index + 1) % len(unique_sites)
        st.session_state.site = unique_sites[next_index]
        st.rerun()

# Add time navigation controls in the sidebar
selected_timestep = render_time_controls(output_db_path, sidebar=True)

# Add site-specific step controls (only shown when paused)
render_site_step_controls(output_db_path, site)

if 'site_chart_counter' not in st.session_state:
    st.session_state.site_chart_counter = 0

# Calculate number of CPUs to build plot
site_cpus = get_site_cpus(sites_info_df, site)

# Create containers for our UI elements
main_col1, main_col2 = st.columns(2)
with main_col1:
    plot_container = st.empty()
with main_col2:
    metrics_container = st.empty()
status_container = st.empty()

# Initial data fetch
with status_container:
    st.info("Reading output database...")

df_site = get_site_data(output_db_path, site=site, max_time=selected_timestep)

# Extract CPU number from host name (format: SITE_NAME_cpu-N)
def extract_cpu_number(cpu_str):
    """Extract CPU number from host name like 'AGLT2_site_X_cpu-N'."""
    try:
        if pd.isna(cpu_str) or not cpu_str:
            return -1
        # Split by 'cpu-' and get the number after it
        if 'cpu-' in cpu_str:
            return int(cpu_str.split('cpu-')[1])
        return -1
    except:
        return -1

if not df_site.empty and 'CPU' in df_site.columns:
    df_site.insert(3, 'CPU #', df_site['CPU'].apply(extract_cpu_number))
else:
    df_site = pd.DataFrame(columns=['JOB_ID', 'STATUS', 'SITE', 'CPU #'])

last_df_hash = hash(str(df_site))

# Initial metrics display
with metrics_container.container():
    active_job_count = get_active_job_count(df_site)
    busy_cpu_count, busy_cpu_pct = get_busy_cpu_count(df_site, site_cpus)

    # Get the Site Data and GFLOPS for the current site
    site_data_row = site_info_json_df[site_info_json_df['Site'] == site]

    if not site_data_row.empty:
        site_data_str = site_data_row['Site Data'].values[0]
        site_GFLOPS = site_data_row['GFLOPS'].values[0]

        # Parse the site data string into a dictionary
        site_data_dict = {}
        if site_data_str:
            # Split by comma and then by colon
            pairs = site_data_str.split(', ')
            for pair in pairs:
                if ': ' in pair:
                    key, value = pair.split(': ', 1)
                    site_data_dict[key] = value

        # Calculate number of columns needed
        # We'll use 2 columns per row for better layout
        num_cols = 2

        # Create a list of all metrics to display
        metrics = [
            {"title": "Active Jobs", "value": active_job_count},
            {"title": "Active CPUs", "value": f"{busy_cpu_count} ({busy_cpu_pct:.1f}%)"},
            {"title": "Site CPUs", "value": site_cpus},
            {"title": "GFLOPS", "value": f"{int(site_GFLOPS):,}"}
        ]

        # Add all site data metrics
        for key, value in site_data_dict.items():
            metrics.append({"title": key, "value": value})

        # Calculate number of rows needed
        num_rows = (len(metrics) + num_cols - 1) // num_cols

        # Create metrics in rows of 2 columns
        for row in range(num_rows):
            cols = st.columns(num_cols)

            # Fill each row with metrics
            for col_idx in range(num_cols):
                metric_idx = row * num_cols + col_idx

                if metric_idx < len(metrics):
                    metric = metrics[metric_idx]
                    cols[col_idx].metric(metric["title"], metric["value"])
    else:
        # Fallback if no site data is found
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Active Jobs", active_job_count)
        with col2:
            st.metric("Active CPUs", f"{busy_cpu_count} ({busy_cpu_pct:.1f}%)")
        with col3:
            st.metric("Site CPUs", site_cpus)

    # Show events dataframe when paused (inside metrics container for aesthetics)
    if not is_live_mode() and selected_timestep is not None:
        st.write("## Events at Current Timestep")
        filter_site_events = st.checkbox(f"Show only events for {site}", value=True, key="filter_site_events")
        events_df = get_events_at_timestep(output_db_path, selected_timestep)
        if not events_df.empty:
            if filter_site_events:
                # Filter to show only events for the current site
                events_df = events_df[events_df['METADATA'].str.contains(f'"{site}"', na=False)]
            if not events_df.empty:
                # Extract CPU number from METADATA host field
                import json
                def extract_cpu_from_metadata(metadata_str):
                    try:
                        metadata = json.loads(metadata_str)
                        host = metadata.get('host', '')
                        return extract_cpu_number(host)
                    except:
                        return -1
                events_df = events_df.copy()
                events_df.insert(1, 'CPU', events_df['METADATA'].apply(extract_cpu_from_metadata))
                st.dataframe(events_df, hide_index=True, width='stretch')
            else:
                st.info(f"No events found for {site} at timestep {selected_timestep:.4f}s")
        else:
            st.info(f"No events found at timestep {selected_timestep:.4f}s")

# Check for pending selection from ANY previous chart widget BEFORE rendering new one
for key in list(st.session_state.keys()):
    if key.startswith('site_chart_'):
        widget_state = st.session_state[key]
        if hasattr(widget_state, 'selection') and widget_state.selection and widget_state.selection.points:
            for point in widget_state.selection.points:
                if point.get('curve_number') == 2:  # Block chart is now trace 2 (after 2 pie charts)
                    cpu_num = point.get('customdata')
                    if cpu_num is not None:
                        st.session_state.cpu = int(cpu_num)
                        del st.session_state[key]
                        st.switch_page('pages/3_CPU.py')
                        st.stop()

# Initial plot render with counter-based key
chart_selection = plot_container.plotly_chart(
    plotly_site_subplot(df_site, site),
    width='stretch',
    on_select="rerun",
    selection_mode="points",
    key=f"site_chart_{st.session_state.site_chart_counter}"
)
st.session_state.site_chart_counter += 1

if is_live_mode():
    status_container.success(f"Monitoring site: {site} (Live)")

# Update loop with live update toggle - use flag to allow clean exit for navigation
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

    # Check for pending selection from ANY previous chart widget (runs in both live and paused modes)
    for key in list(st.session_state.keys()):
        if key.startswith('site_chart_'):
            widget_state = st.session_state[key]
            if hasattr(widget_state, 'selection') and widget_state.selection and widget_state.selection.points:
                for point in widget_state.selection.points:
                    if point.get('curve_number') == 2:  # Block chart is now trace 2 (after 2 pie charts)
                        cpu_num = point.get('customdata')
                        if cpu_num is not None:
                            st.session_state.cpu = int(cpu_num)
                            del st.session_state[key]
                            navigate_to_page = 'pages/3_CPU.py'
                            should_exit_loop = True
                            break
            if should_exit_loop:
                break

    if should_exit_loop:
        continue

    # Only fetch and update if in live (playing) mode
    if is_live_mode():
        try:
            # Get new data (no time filter in live mode)
            new_df_site = get_site_data(output_db_path, site=site)
            if not new_df_site.empty and 'CPU' in new_df_site.columns:
                new_df_site.insert(3, 'CPU #', new_df_site['CPU'].apply(extract_cpu_number))
            else:
                new_df_site = pd.DataFrame(columns=['JOB_ID', 'STATUS', 'SITE', 'CPU #'])
            new_df_hash = hash(str(new_df_site))

            # Only update if data changed
            if new_df_hash != last_df_hash:
                # Update metrics first (smaller UI element)
                with metrics_container.container():
                    new_active_job_count = get_active_job_count(new_df_site)
                    new_busy_cpu_count, new_busy_cpu_pct = get_busy_cpu_count(new_df_site, site_cpus)

                    # Get the Site Data and GFLOPS for the current site
                    site_data_row = site_info_json_df[site_info_json_df['Site'] == site]

                    if not site_data_row.empty:
                        site_data_str = site_data_row['Site Data'].values[0]
                        site_GFLOPS = site_data_row['GFLOPS'].values[0]

                        # Parse the site data string into a dictionary
                        site_data_dict = {}
                        if site_data_str:
                            # Split by comma and then by colon
                            pairs = site_data_str.split(', ')
                            for pair in pairs:
                                if ': ' in pair:
                                    key, value = pair.split(': ', 1)
                                    site_data_dict[key] = value

                        # Calculate number of columns needed
                        # We'll use 2 columns per row for better layout
                        num_cols = 2

                        # Create a list of all metrics to display
                        metrics = [
                            {"title": "Active Jobs", "value": new_active_job_count, "delta": new_active_job_count - active_job_count},
                            {"title": "Active CPUs", "value": f"{new_busy_cpu_count} ({new_busy_cpu_pct:.1f}%)"},
                            {"title": "Site CPUs", "value": site_cpus},
                            {"title": "GFLOPS", "value": f"{int(site_GFLOPS):,}"}
                        ]

                        # Add all site data metrics
                        for key, value in site_data_dict.items():
                            metrics.append({"title": key, "value": value})

                        # Calculate number of rows needed
                        num_rows = (len(metrics) + num_cols - 1) // num_cols

                        # Create metrics in rows of 2 columns
                        for row in range(num_rows):
                            cols = st.columns(num_cols)

                            # Fill each row with metrics
                            for col_idx in range(num_cols):
                                metric_idx = row * num_cols + col_idx

                                if metric_idx < len(metrics):
                                    metric = metrics[metric_idx]
                                    if "delta" in metric:
                                        cols[col_idx].metric(metric["title"], metric["value"], delta=metric["delta"])
                                    else:
                                        cols[col_idx].metric(metric["title"], metric["value"])
                    else:
                        # Fallback if no site data is found
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Active Jobs", new_active_job_count,
                                    delta=new_active_job_count - active_job_count)
                        with col2:
                            st.metric("Active CPUs", f"{new_busy_cpu_count} ({new_busy_cpu_pct:.1f}%)")
                        with col3:
                            st.metric("Site CPUs", site_cpus)

                    active_job_count = new_active_job_count

                # Check for pending selection from ANY previous chart widget
                for key in list(st.session_state.keys()):
                    if key.startswith('site_chart_'):
                        widget_state = st.session_state[key]
                        if hasattr(widget_state, 'selection') and widget_state.selection and widget_state.selection.points:
                            for point in widget_state.selection.points:
                                if point.get('curve_number') == 2:  # Block chart is now trace 2 (after 2 pie charts)
                                    cpu_num = point.get('customdata')
                                    if cpu_num is not None:
                                        st.session_state.cpu = int(cpu_num)
                                        del st.session_state[key]
                                        navigate_to_page = 'pages/3_CPU.py'
                                        should_exit_loop = True
                                        break
                        if should_exit_loop:
                            break

                if should_exit_loop:
                    continue

                # Then update the plot with counter-based key
                live_chart_selection = plot_container.plotly_chart(
                    plotly_site_subplot(new_df_site, site),
                    width='content',
                    on_select="rerun",
                    selection_mode="points",
                    key=f"site_chart_{st.session_state.site_chart_counter}"
                )
                st.session_state.site_chart_counter += 1

                last_df_hash = new_df_hash

                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating: {e}")
            time.sleep(1)  # Wait a bit longer on error

# After loop exits (when navigation is needed), perform the navigation
if navigate_to_page:
    st.switch_page(navigate_to_page)
