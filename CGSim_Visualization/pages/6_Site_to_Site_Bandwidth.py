import time, json, sqlite3, os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from vis_utils.time_controls import (
    render_time_controls, is_live_mode, update_timestep_metric,
    get_available_timesteps, get_transfer_timesteps_for_pair
)

st.set_page_config(layout="wide",
                   page_title="CGSim - Site to Site",
                   page_icon="🔀")

# Check for pending navigation FIRST before anything else
if 'pending_navigation' in st.session_state and st.session_state.pending_navigation:
    target_page = st.session_state.pending_navigation
    st.session_state.pending_navigation = None
    st.switch_page(target_page)

# ─── Configuration Loading ────────────────────────────────────────────────────

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

    st.session_state.output_dir             = saved_config.get("output_dir",             DEFAULT_CONFIG["output_dir"])
    st.session_state.output_filename        = saved_config.get("output_filename",        DEFAULT_CONFIG["output_filename"])
    st.session_state.config_dir             = saved_config.get("config_dir",             DEFAULT_CONFIG["config_dir"])
    st.session_state.selected_config_file   = saved_config.get("selected_config_file",   DEFAULT_CONFIG["selected_config_file"])
    st.session_state.output_db_path         = os.path.join(st.session_state.output_dir, st.session_state.output_filename)
    st.session_state.site_info_json_path    = os.path.join(st.session_state.config_dir, st.session_state.selected_config_file)
    st.session_state.config_loaded = True

if "output_db_path" not in st.session_state:
    st.session_state.output_db_path = os.path.join(DEFAULT_CONFIG["output_dir"], DEFAULT_CONFIG["output_filename"])

if "site_info_json_path" not in st.session_state:
    st.session_state.site_info_json_path = os.path.join(DEFAULT_CONFIG["config_dir"], DEFAULT_CONFIG["selected_config_file"])

output_db_path       = st.session_state.output_db_path
site_info_json_path  = st.session_state.site_info_json_path
global_sleep_time    = 0.5  # sec

# ─── Helpers ──────────────────────────────────────────────────────────────────

@st.cache_data
def get_all_site_names(site_info_json_path):
    """Return sorted list of all site names from the config JSON."""
    if not os.path.exists(site_info_json_path):
        return []
    try:
        with open(site_info_json_path, 'r') as f:
            sites_info = json.load(f)
        return sorted(sites_info.keys())
    except Exception:
        return []


def bytes_to_human(b):
    """Convert bytes to a human-readable string."""
    if b == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if b < 1024.0:
            return f"{b:.2f} {unit}"
        b /= 1024.0
    return f"{b:.2f} PB"


@st.cache_data(ttl=global_sleep_time)
def get_transfers_between(output_db_path, source_site, dest_site, max_time=None):
    """
    Fetch all FileTransfer events between source_site and dest_site.

    Returns a DataFrame with columns:
        TIME, file, size, bandwidth, latency, link_load,
        grid_storage_util, site_storage_util
    """
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""
        query = f"""
            SELECT TIME, METADATA
            FROM EVENTS
            WHERE EVENT = 'FileTransfer'
            {time_filter}
            AND METADATA LIKE ?
            AND METADATA LIKE ?
            ORDER BY TIME
        """
        src_pat = f'%"source_site":"{source_site}"%'
        dst_pat = f'%"destination_site":"{dest_site}"%'
        df = pd.read_sql_query(query, conn, params=(src_pat, dst_pat))
        conn.close()

        if df.empty:
            return pd.DataFrame()

        def parse(s):
            try:
                return json.loads(s)
            except Exception:
                return {}

        parsed = df['METADATA'].apply(parse)
        df['File']               = parsed.apply(lambda x: x.get('file', ''))
        df['Size']               = parsed.apply(lambda x: float(x.get('size', 0)))
        df['Bandwidth (B/s)']    = parsed.apply(lambda x: float(x.get('bandwidth', 0)))
        df['Latency (s)']        = parsed.apply(lambda x: float(x.get('latency', 0)))
        df['Link Load']          = parsed.apply(lambda x: float(x.get('link_load', 0)))
        df['Grid Storage Util']  = parsed.apply(lambda x: float(x.get('grid_storage_util', 0)))
        df['Site Storage Util']  = parsed.apply(lambda x: float(x.get('site_storage_util', 0)))
        df = df.drop(columns=['METADATA'])
        df = df.rename(columns={'TIME': 'Timestep (s)'})
        df['Size (Human)'] = df['Size'].apply(bytes_to_human)
        df['Bandwidth']    = df['Bandwidth (B/s)'].apply(bytes_to_human).apply(lambda s: s + '/s')
        return df
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=global_sleep_time)
def get_filewrites_at_sites(output_db_path, source_site, dest_site, max_time=None):
    """
    Fetch FileWrite events at either source_site or dest_site.

    Returns a DataFrame with columns:
        TIME, site, host, file, size, disk, disk_write_bw, site_storage_util
    """
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""
        src_pat = f'%"site":"{source_site}"%'
        dst_pat = f'%"site":"{dest_site}"%'
        query = f"""
            SELECT TIME, METADATA
            FROM EVENTS
            WHERE EVENT = 'FileWrite'
            {time_filter}
            AND (METADATA LIKE ? OR METADATA LIKE ?)
            ORDER BY TIME
        """
        df = pd.read_sql_query(query, conn, params=(src_pat, dst_pat))
        conn.close()

        if df.empty:
            return pd.DataFrame()

        def parse(s):
            try:
                return json.loads(s)
            except Exception:
                return {}

        parsed = df['METADATA'].apply(parse)
        df['Site']               = parsed.apply(lambda x: x.get('site', ''))
        df['Host']               = parsed.apply(lambda x: x.get('host', ''))
        df['File']               = parsed.apply(lambda x: x.get('file', ''))
        df['Size']               = parsed.apply(lambda x: float(x.get('size', 0)))
        df['Disk']               = parsed.apply(lambda x: x.get('disk', ''))
        df['Disk Write BW (B/s)']= parsed.apply(lambda x: float(x.get('disk_write_bw', 0)))
        df['Site Storage Util']  = parsed.apply(lambda x: float(x.get('site_storage_util', 0)))
        df = df.drop(columns=['METADATA'])
        df = df.rename(columns={'TIME': 'Timestep (s)'})
        df['Size (Human)']    = df['Size'].apply(bytes_to_human)
        df['Disk Write BW']   = df['Disk Write BW (B/s)'].apply(bytes_to_human).apply(lambda s: s + '/s')
        return df
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def build_transfer_timeline(df_transfers):
    """Build a Plotly scatter timeline of file transfers over time."""
    if df_transfers.empty:
        fig = go.Figure()
        fig.update_layout(
            title="No transfers recorded yet",
            paper_bgcolor='black',
            plot_bgcolor='#111',
            font=dict(color='white'),
            height=300,
        )
        return fig

    hover = (
        "Time: %{x:.4f}s<br>"
        "Size: %{customdata[0]}<br>"
        "Bandwidth: %{customdata[1]}<br>"
        "File: %{customdata[2]}"
        "<extra></extra>"
    )

    fig = go.Figure(data=go.Scatter(
        x=df_transfers['Timestep (s)'],
        y=df_transfers['Size'] / 1e9,
        mode='markers',
        marker=dict(
            size=11,
            color=df_transfers['Bandwidth (B/s)'],
            colorscale='Plasma',
            showscale=True,
            colorbar=dict(
                title=dict(text="Bandwidth (B/s)", font=dict(color='white', size=11)),
                tickfont=dict(color='white'),
            ),
            line=dict(color='rgba(255,255,255,0.3)', width=0.5),
        ),
        customdata=list(zip(
            df_transfers['Size (Human)'],
            df_transfers['Bandwidth'],
            df_transfers['File'],
        )),
        hovertemplate=hover,
    ))

    fig.update_layout(
        title=dict(
            text="<b>Transfer Events Over Time</b>",
            font=dict(size=18, color='white'),
            x=0.5, xanchor='center',
        ),
        xaxis=dict(
            title=dict(text="Simulation Time (s)", font=dict(color='#aaa', size=13)),
            tickfont=dict(color='white'),
            gridcolor='rgba(255,255,255,0.07)',
        ),
        yaxis=dict(
            title=dict(text="Transfer Size (GB)", font=dict(color='#aaa', size=13)),
            tickfont=dict(color='white'),
            gridcolor='rgba(255,255,255,0.07)',
        ),
        paper_bgcolor='black',
        plot_bgcolor='#0e0e0e',
        font=dict(color='white'),
        margin=dict(l=20, r=20, t=50, b=40),
        hoverlabel=dict(bgcolor='#1e1e2e', bordercolor='#7c3aed',
                        font=dict(color='white', size=12)),
        height=360,
    )
    return fig


# ─── Sidebar step controls for this site pair ─────────────────────────────────

def render_pair_step_controls(db_path, source_site, dest_site):
    """Back/Fwd buttons that step through FileTransfer timesteps for this pair."""
    from vis_utils.time_controls import init_time_state, get_available_timesteps
    init_time_state()

    # Only show when paused
    if st.session_state.get('time_playing', True):
        return

    pair_timesteps = get_transfer_timesteps_for_pair(db_path, source_site, dest_site)
    if not pair_timesteps:
        st.sidebar.info("No transfer events found for this pair.")
        return

    current_ts = st.session_state.get('selected_timestep')
    if current_ts is None:
        return

    st.sidebar.write("### Site Pair Transfer Steps")

    prev_ts = max((t for t in pair_timesteps if t < current_ts), default=None)
    next_ts = min((t for t in pair_timesteps if t > current_ts), default=None)

    all_timesteps = get_available_timesteps(db_path)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("⏮️ Prev Transfer", key="s2s_back_btn",
                     disabled=prev_ts is None, width='stretch'):
            st.session_state.selected_timestep = prev_ts
            try:
                st.session_state.timestep_index = all_timesteps.index(prev_ts)
            except ValueError:
                st.session_state.timestep_index = min(
                    range(len(all_timesteps)),
                    key=lambda i: abs(all_timesteps[i] - prev_ts)
                )
            st.rerun()

    with col2:
        if st.button("⏭️ Next Transfer", key="s2s_fwd_btn",
                     disabled=next_ts is None, width='stretch'):
            st.session_state.selected_timestep = next_ts
            try:
                st.session_state.timestep_index = all_timesteps.index(next_ts)
            except ValueError:
                st.session_state.timestep_index = min(
                    range(len(all_timesteps)),
                    key=lambda i: abs(all_timesteps[i] - next_ts)
                )
            st.rerun()

    total_pair = len(pair_timesteps)
    idx_in_pair = sum(1 for t in pair_timesteps if t <= current_ts)
    st.sidebar.caption(f"Transfer event {idx_in_pair} / {total_pair} for this pair")


# ─── Site & pair selection ────────────────────────────────────────────────────

all_sites = get_all_site_names(site_info_json_path)

# Defaults — set by heatmap cell click or fallback to first two sites
if 's2s_source' not in st.session_state:
    st.session_state.s2s_source = all_sites[0] if len(all_sites) > 0 else ''
if 's2s_dest' not in st.session_state:
    st.session_state.s2s_dest = all_sites[1] if len(all_sites) > 1 else ''

# ─── Header ───────────────────────────────────────────────────────────────────

st.write("# CGSim Visualization")

# Sidebar navigation
st.sidebar.write("## Navigation")
nav_col1, nav_col2 = st.sidebar.columns(2)
with nav_col1:
    if st.button("Overview", width='stretch'):
        st.switch_page('CGSim_Visualization.py')
with nav_col2:
    if st.button("Heatmap", width='stretch'):
        st.switch_page('pages/5_Bandwidth_Heatmap.py')

# Sidebar site pair selection
st.sidebar.write("## Site Pair Selection")

def on_source_change():
    st.session_state.s2s_source = st.session_state._s2s_src_select

def on_dest_change():
    st.session_state.s2s_dest = st.session_state._s2s_dst_select

src_idx = all_sites.index(st.session_state.s2s_source) if st.session_state.s2s_source in all_sites else 0
dst_idx = all_sites.index(st.session_state.s2s_dest)   if st.session_state.s2s_dest   in all_sites else (1 if len(all_sites) > 1 else 0)

st.sidebar.selectbox(
    "Source Site:",
    all_sites,
    index=src_idx,
    key="_s2s_src_select",
    on_change=on_source_change,
)
st.sidebar.selectbox(
    "Destination Site:",
    all_sites,
    index=dst_idx,
    key="_s2s_dst_select",
    on_change=on_dest_change,
)

source_site = st.session_state.s2s_source
dest_site   = st.session_state.s2s_dest

st.write(f"## 🔀 {source_site}  →  {dest_site}")
st.caption("File transfer and write activity between the selected site pair.")

# Time controls and pair-specific step navigation
selected_timestep = render_time_controls(output_db_path, sidebar=True)
render_pair_step_controls(output_db_path, source_site, dest_site)

# ─── Containers ───────────────────────────────────────────────────────────────

metrics_container  = st.empty()
timeline_container = st.empty()
status_container   = st.empty()
transfers_header   = st.empty()
transfers_df_cont  = st.empty()
writes_header      = st.empty()
writes_df_cont     = st.empty()

# widget key counter
if 's2s_chart_counter' not in st.session_state:
    st.session_state.s2s_chart_counter = 0


# ─── Render function ──────────────────────────────────────────────────────────

def render_page(df_transfers, df_writes, source_site, dest_site):
    """Render metrics, timeline, and raw data tables."""

    # ── Summary metrics ──────────────────────────────────────────────────────
    total_transfers   = len(df_transfers)
    total_bytes       = df_transfers['Size'].sum() if not df_transfers.empty else 0.0
    avg_bw            = df_transfers['Bandwidth (B/s)'].mean() if not df_transfers.empty else 0.0
    total_writes      = len(df_writes)

    with metrics_container.container():
        st.markdown("""
        <style>
        div[data-testid="metric-container"] {
            background: linear-gradient(135deg, #0d1b2a 0%, #0a0a1a 100%);
            border: 1px solid rgba(56, 189, 248, 0.35);
            border-radius: 10px;
            padding: 14px 18px 10px 18px;
        }
        div[data-testid="metric-container"] label {
            color: #7dd3fc !important;
            font-size: 0.78rem !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            color: white !important;
            font-size: 1.4rem !important;
            font-weight: 700;
        }
        </style>
        """, unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("File Transfers",    f"{total_transfers:,}")
        c2.metric("Total Data Sent",   bytes_to_human(total_bytes))
        c3.metric("Avg Bandwidth",     bytes_to_human(avg_bw) + "/s" if avg_bw else "N/A")
        c4.metric("File Writes (Either Site)", f"{total_writes:,}")

    # ── Timeline ──────────────────────────────────────────────────────────────
    fig = build_transfer_timeline(df_transfers)
    timeline_container.plotly_chart(
        fig,
        use_container_width=True,
        key=f"s2s_timeline_{st.session_state.s2s_chart_counter}"
    )
    st.session_state.s2s_chart_counter += 1

    # ── FileTransfer raw table ────────────────────────────────────────────────
    with transfers_header.container():
        st.write(f"### FileTransfer Events: {source_site} → {dest_site}")
    with transfers_df_cont.container():
        if not df_transfers.empty:
            display_cols = ['Timestep (s)', 'File', 'Size (Human)', 'Bandwidth',
                            'Latency (s)', 'Link Load', 'Grid Storage Util', 'Site Storage Util']
            st.dataframe(
                df_transfers[display_cols].sort_values('Timestep (s)', ascending=False),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info(f"No FileTransfer events found between {source_site} and {dest_site}.")

    # ── FileWrite raw table ───────────────────────────────────────────────────
    with writes_header.container():
        st.write(f"### FileWrite Events at Either Site")
    with writes_df_cont.container():
        if not df_writes.empty:
            display_cols = ['Timestep (s)', 'Site', 'Host', 'File', 'Size (Human)',
                            'Disk Write BW', 'Disk', 'Site Storage Util']
            st.dataframe(
                df_writes[display_cols].sort_values('Timestep (s)', ascending=False),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info(f"No FileWrite events found at {source_site} or {dest_site}.")


# ─── Initial Render ───────────────────────────────────────────────────────────

with status_container:
    st.info("Reading output database...")

df_transfers = get_transfers_between(output_db_path, source_site, dest_site, max_time=selected_timestep)
df_writes    = get_filewrites_at_sites(output_db_path, source_site, dest_site, max_time=selected_timestep)

status_container.empty()
render_page(df_transfers, df_writes, source_site, dest_site)

last_hash = hash(str(df_transfers) + str(df_writes))

if is_live_mode():
    status_container.success(f"Monitoring {source_site} → {dest_site} (Live)")

# ─── Live Update Loop ─────────────────────────────────────────────────────────

last_selected_timestep = st.session_state.get('selected_timestep')

while True:
    time.sleep(global_sleep_time)
    update_timestep_metric(output_db_path)

    # Rerun if timestep changed (from step-nav buttons)
    current_ts = st.session_state.get('selected_timestep')
    if current_ts != last_selected_timestep:
        st.rerun()

    if is_live_mode():
        try:
            new_df_t = get_transfers_between(output_db_path, source_site, dest_site)
            new_df_w = get_filewrites_at_sites(output_db_path, source_site, dest_site)
            new_hash = hash(str(new_df_t) + str(new_df_w))

            if new_hash != last_hash:
                last_hash = new_hash
                render_page(new_df_t, new_df_w, source_site, dest_site)
                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating: {e}")
            time.sleep(1)
