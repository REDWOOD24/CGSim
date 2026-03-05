import time, json, sqlite3, os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from vis_utils.time_controls import (
    render_time_controls, is_live_mode, update_timestep_metric,
    init_time_state, get_available_timesteps, get_all_filewrite_timesteps,
    get_events_at_timestep
)

st.set_page_config(layout="wide",
                   page_title="CGSim - Bandwidth Heatmap",
                   page_icon="🌡️")

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

    st.session_state.output_dir = saved_config.get("output_dir", DEFAULT_CONFIG["output_dir"])
    st.session_state.output_filename = saved_config.get("output_filename", DEFAULT_CONFIG["output_filename"])
    st.session_state.config_dir = saved_config.get("config_dir", DEFAULT_CONFIG["config_dir"])
    st.session_state.selected_config_file = saved_config.get("selected_config_file", DEFAULT_CONFIG["selected_config_file"])
    st.session_state.output_db_path = os.path.join(st.session_state.output_dir, st.session_state.output_filename)
    st.session_state.site_info_json_path = os.path.join(st.session_state.config_dir, st.session_state.selected_config_file)
    st.session_state.config_loaded = True

if "output_db_path" not in st.session_state:
    st.session_state.output_db_path = os.path.join(DEFAULT_CONFIG["output_dir"], DEFAULT_CONFIG["output_filename"])

if "site_info_json_path" not in st.session_state:
    st.session_state.site_info_json_path = os.path.join(DEFAULT_CONFIG["config_dir"], DEFAULT_CONFIG["selected_config_file"])

output_db_path = st.session_state.output_db_path
site_info_json_path = st.session_state.site_info_json_path
global_sleep_time = 0.5  # sec

# ─── Data Functions ───────────────────────────────────────────────────────────

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


@st.cache_data(ttl=global_sleep_time)
def get_transfer_data(output_db_path, max_time=None):
    """
    Query all FileTransfer events and return a DataFrame with columns:
    source_site, destination_site, size (bytes), bandwidth, file, TIME.

    Args:
        output_db_path: Path to the SQLite database.
        max_time: Optional upper time bound; None means all events.

    Returns:
        DataFrame of file transfer rows, or empty DataFrame on error.
    """
    if not os.path.exists(output_db_path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(output_db_path)
        time_filter = f"AND TIME <= {max_time}" if max_time is not None else ""
        query = f"""
            SELECT
                JOB_ID,
                TIME,
                METADATA
            FROM EVENTS
            WHERE EVENT = 'FileTransfer'
            {time_filter}
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return pd.DataFrame()

        # Parse METADATA JSON
        def parse_metadata(metadata_str):
            try:
                return json.loads(metadata_str)
            except Exception:
                return {}

        parsed = df['METADATA'].apply(parse_metadata)
        df['source_site']      = parsed.apply(lambda x: x.get('source_site', ''))
        df['destination_site'] = parsed.apply(lambda x: x.get('destination_site', ''))
        df['size']             = parsed.apply(lambda x: float(x.get('size', 0)))
        df['bandwidth']        = parsed.apply(lambda x: float(x.get('bandwidth', 0)))
        df['file']             = parsed.apply(lambda x: x.get('file', ''))

        df = df.drop(columns=['METADATA'])
        df = df[df['source_site'] != '']
        df = df[df['destination_site'] != '']

        return df
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def build_heatmap_matrix(df_transfers, all_sites):
    """
    Build an N×N matrix of total bytes transferred from source (row) to
    destination (col) for every site pair.

    Returns:
        z_matrix (np.ndarray): shape (N, N) total bytes
        count_matrix (np.ndarray): shape (N, N) number of transfers
    """
    n = len(all_sites)
    site_idx = {site: i for i, site in enumerate(all_sites)}

    z_matrix     = np.zeros((n, n), dtype=float)
    count_matrix = np.zeros((n, n), dtype=int)

    if df_transfers.empty:
        return z_matrix, count_matrix

    grouped = (
        df_transfers
        .groupby(['source_site', 'destination_site'])
        .agg(total_size=('size', 'sum'), count=('size', 'count'))
        .reset_index()
    )

    for _, row in grouped.iterrows():
        src = row['source_site']
        dst = row['destination_site']
        if src in site_idx and dst in site_idx:
            i = site_idx[src]
            j = site_idx[dst]
            z_matrix[i][j]     = row['total_size']
            count_matrix[i][j] = row['count']

    return z_matrix, count_matrix


def bytes_to_human(b):
    """Convert bytes to a human-readable string."""
    if b == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if b < 1024.0:
            return f"{b:.2f} {unit}"
        b /= 1024.0
    return f"{b:.2f} PB"


def build_heatmap_fig(z_matrix, count_matrix, all_sites):
    """
    Build a scatter-square grid that LOOKS like a heatmap but uses go.Scatter
    with selectable points — required for Streamlit on_select to fire.
    (go.Heatmap does not participate in Plotly's point-selection model.)

    Rows (Y) = source site, Cols (X) = destination site.
    customdata on each point = [src_site, dst_site] for click detection.
    """
    n = len(all_sites)

    # ── Dynamic sizing based on site count ────────────────────────────────────
    px_per_site     = max(14, min(28, int(900 / max(n, 1))))
    chart_height    = max(700, n * px_per_site + 200)
    tick_font_size  = max(8, min(14, int(420 / max(n, 1))))
    label_font_size = max(12, min(18, tick_font_size + 4))
    marker_size     = max(6, px_per_site - 3)

    z_max = z_matrix.max() if z_matrix.max() > 0 else 1.0

    # Flatten the N x N grid into parallel arrays for Scatter
    xs, ys, colors, hover_texts, custom = [], [], [], [], []
    for i, src in enumerate(all_sites):     # row = source (y-axis)
        for j, dst in enumerate(all_sites): # col = dest  (x-axis)
            total_b = z_matrix[i][j]
            cnt     = count_matrix[i][j]
            xs.append(j)
            ys.append(i)
            colors.append(total_b)
            hover_texts.append(
                f"<b>Source:</b> {src}<br>"
                f"<b>Destination:</b> {dst}<br>"
                f"<b>Total Transferred:</b> {bytes_to_human(total_b)}<br>"
                f"<b>File Transfers:</b> {cnt:,}<br>"
                f"<i>Click to open Site-to-Site detail</i>"
            )
            custom.append([src, dst])

    fig = go.Figure(data=go.Scatter(
        x=xs,
        y=ys,
        mode='markers',
        marker=dict(
            symbol='square',
            size=marker_size,
            color=colors,
            colorscale='Plasma',
            cmin=0,
            cmax=z_max,
            showscale=True,
            colorbar=dict(
                title=dict(
                    text="Data Transferred (B)",
                    side="right",
                    font=dict(color='white', size=13),
                ),
                tickfont=dict(color='white'),
                outlinecolor='rgba(255,255,255,0.2)',
                outlinewidth=1,
            ),
            line=dict(width=0),
        ),
        text=hover_texts,
        hoverinfo='text',
        customdata=custom,
        showlegend=False,
    ))

    fig.update_layout(
        title=dict(
            text="<b>Site-to-Site File Transfer Volume</b>  ·  <i style='font-size:14px'>click a cell to drill in</i>",
            font=dict(size=20, color='white'),
            x=0.5, xanchor='center', y=0.99, yanchor='top',
        ),
        xaxis=dict(
            title=dict(text="<b>Destination Site</b>", font=dict(color='#aaa', size=label_font_size)),
            tickfont=dict(color='white', size=tick_font_size),
            tickangle=-45,
            tickmode='array',
            tickvals=list(range(n)),
            ticktext=all_sites,
            gridcolor='rgba(255,255,255,0.04)',
            zeroline=False,
            range=[-0.5, n - 0.5],
        ),
        yaxis=dict(
            title=dict(text="<b>Source Site</b>", font=dict(color='#aaa', size=label_font_size)),
            tickfont=dict(color='white', size=tick_font_size),
            tickmode='array',
            tickvals=list(range(n)),
            ticktext=all_sites,
            autorange='reversed',
            gridcolor='rgba(255,255,255,0.04)',
            zeroline=False,
            range=[n - 0.5, -0.5],
        ),
        paper_bgcolor='black',
        plot_bgcolor='#0e0e0e',
        font=dict(color='white'),
        margin=dict(l=20, r=20, t=60, b=max(120, tick_font_size * 10)),
        hoverlabel=dict(
            bgcolor='#1e1e2e',
            bordercolor='#7c3aed',
            font=dict(color='white', size=13, family='Arial'),
        ),
        height=chart_height,
    )

    return fig


def compute_summary_metrics(df_transfers):
    """Compute aggregate metrics for display above the heatmap."""
    if df_transfers.empty:
        return 0, 0.0, "N/A", 0
    total_transfers = len(df_transfers)
    total_bytes     = df_transfers['size'].sum()
    busiest_src     = df_transfers.groupby('source_site')['size'].sum().idxmax()
    busiest_bytes   = df_transfers.groupby('source_site')['size'].sum().max()
    return total_transfers, total_bytes, busiest_src, busiest_bytes


# ─── Global FileWrite Step Controls ─────────────────────────────────────────

def render_filewrite_step_controls(db_path):
    """Show FileWrite Prev/Next buttons in sidebar when paused (sim-wide, all sites)."""
    init_time_state()

    # Only show when paused
    if st.session_state.get('time_playing', True):
        return

    fw_timesteps = get_all_filewrite_timesteps(db_path)
    if not fw_timesteps:
        st.sidebar.info("No FileWrite events found yet.")
        return

    current_ts = st.session_state.get('selected_timestep')
    if current_ts is None:
        return

    st.sidebar.write("### FileWrite Steps")

    prev_ts = max((t for t in fw_timesteps if t < current_ts), default=None)
    next_ts = min((t for t in fw_timesteps if t > current_ts), default=None)

    all_ts = get_available_timesteps(db_path)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("⏮️ Prev Write", key="hm_fw_back_btn",
                     disabled=prev_ts is None, width='stretch'):
            st.session_state.selected_timestep = prev_ts
            try:
                st.session_state.timestep_index = all_ts.index(prev_ts)
            except ValueError:
                st.session_state.timestep_index = min(
                    range(len(all_ts)), key=lambda i: abs(all_ts[i] - prev_ts)
                )
            st.rerun()
    with col2:
        if st.button("⏭️ Next Write", key="hm_fw_fwd_btn",
                     disabled=next_ts is None, width='stretch'):
            st.session_state.selected_timestep = next_ts
            try:
                st.session_state.timestep_index = all_ts.index(next_ts)
            except ValueError:
                st.session_state.timestep_index = min(
                    range(len(all_ts)), key=lambda i: abs(all_ts[i] - next_ts)
                )
            st.rerun()

    total_fw   = len(fw_timesteps)
    idx_in_fw  = sum(1 for t in fw_timesteps if t <= current_ts)
    st.sidebar.caption(f"FileWrite event {idx_in_fw} / {total_fw} (all sites)")


# ─── Page Layout ─────────────────────────────────────────────────────────────

st.write("# CGSim Visualization")
st.write("## Bandwidth Heatmap")
st.caption("Cumulative file transfer volume between sites. Hover a cell for details. **Click a cell** to drill into that site pair.")

# Sidebar time controls
selected_timestep = render_time_controls(output_db_path, sidebar=True)
render_filewrite_step_controls(output_db_path)

# Load site list from config
all_sites = get_all_site_names(site_info_json_path)

# Check for pending navigation AFTER widgets are set up
if st.session_state.get('pending_navigation'):
    target = st.session_state.pending_navigation
    st.session_state.pending_navigation = None
    st.switch_page(target)

# Containers
metrics_container = st.empty()
status_container  = st.empty()
heatmap_container = st.empty()

# Counter for Plotly widget key uniqueness
if 'bw_heatmap_counter' not in st.session_state:
    st.session_state.bw_heatmap_counter = 0


def render_metrics(df_transfers):
    """Render summary metric boxes."""
    total_transfers, total_bytes, busiest_src, busiest_bytes = compute_summary_metrics(df_transfers)
    with metrics_container.container():
        st.markdown("""
        <style>
        div[data-testid="metric-container"] {
            background: linear-gradient(135deg, #1a0533 0%, #0d0d1a 100%);
            border: 1px solid rgba(124, 58, 237, 0.4);
            border-radius: 10px;
            padding: 14px 18px 10px 18px;
        }
        div[data-testid="metric-container"] label {
            color: #a78bfa !important;
            font-size: 0.78rem !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            color: white !important;
            font-size: 1.45rem !important;
            font-weight: 700;
        }
        </style>
        """, unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total File Transfers",       f"{total_transfers:,}")
        c2.metric("Total Data Transferred",      bytes_to_human(total_bytes))
        c3.metric("Busiest Source (All Dests)",  busiest_src if total_transfers > 0 else "N/A")
        c4.metric("Busiest Site Total Sent",     bytes_to_human(busiest_bytes) if total_transfers > 0 else "N/A")


# ─── Initial Data Fetch ───────────────────────────────────────────────────────

with status_container:
    st.info("Reading output database...")

df_transfers = get_transfer_data(output_db_path, max_time=selected_timestep)

if df_transfers.empty and not os.path.exists(output_db_path):
    status_container.error(
        f"Database not found at `{output_db_path}`. "
        "Please check your configuration on the Configuration page."
    )
else:
    if df_transfers.empty:
        status_container.warning("No FileTransfer events found yet. The simulation may still be starting up.")
    else:
        status_container.empty()

    # Derive site list from actual data when config JSON is unavailable
    if not all_sites and not df_transfers.empty:
        all_sites = sorted(
            set(df_transfers['source_site'].tolist() + df_transfers['destination_site'].tolist())
        )

last_df_hash = hash(str(df_transfers))

# ─── Check for pending cell click BEFORE rendering the new chart ──────────────
# Mirrors the Site→CPU pattern: detect selection from the previous widget instance,
# then navigate before the new chart is drawn.
for key in list(st.session_state.keys()):
    if key.startswith('bw_heatmap_'):
        widget_state = st.session_state[key]
        if hasattr(widget_state, 'selection') and widget_state.selection and widget_state.selection.points:
            for point in widget_state.selection.points:
                # customdata = [src_site, dst_site] stored on each scatter point
                cd = point.get('customdata')
                if cd and len(cd) >= 2:
                    src_site, dst_site = cd[0], cd[1]
                    if src_site in all_sites and dst_site in all_sites:
                        st.session_state.s2s_source = src_site
                        st.session_state.s2s_dest   = dst_site
                        del st.session_state[key]
                        st.switch_page('pages/6_Site_to_Site_Bandwidth.py')
                        st.stop()

# ─── Render metrics + heatmap chart ──────────────────────────────────────────

if all_sites:
    render_metrics(df_transfers)
    z_matrix, count_matrix = build_heatmap_matrix(df_transfers, all_sites)
    fig = build_heatmap_fig(z_matrix, count_matrix, all_sites)
    heatmap_container.plotly_chart(
        fig,
        config={"width": "stretch"},
        on_select="rerun",
        selection_mode="points",
        key=f"bw_heatmap_{st.session_state.bw_heatmap_counter}"
    )
    st.session_state.bw_heatmap_counter += 1
else:
    st.warning("No site configuration found. Please check your Configuration settings.")

if is_live_mode():
    status_container.success("Monitoring transfers (Live)")

# ─── Paused: FileWrite events at current timestep ────────────────────────────

if not is_live_mode() and selected_timestep is not None:
    st.write("### FileWrite Events at Current Timestep")
    fw_df = get_events_at_timestep(output_db_path, selected_timestep)
    fw_df = fw_df[fw_df['EVENT'] == 'FileWrite'] if not fw_df.empty else fw_df

    if not fw_df.empty:
        # Parse METADATA into readable columns
        def parse_fw(s):
            try:
                return json.loads(s)
            except Exception:
                return {}
        parsed = fw_df['METADATA'].apply(parse_fw)
        fw_df = fw_df.copy()
        fw_df['Site']            = parsed.apply(lambda x: x.get('site', ''))
        fw_df['Host']            = parsed.apply(lambda x: x.get('host', ''))
        fw_df['File']            = parsed.apply(lambda x: x.get('file', ''))
        fw_df['Size']            = parsed.apply(lambda x: float(x.get('size', 0)))
        fw_df['Disk']            = parsed.apply(lambda x: x.get('disk', ''))
        fw_df['Disk Write BW']   = parsed.apply(
            lambda x: bytes_to_human(float(x.get('disk_write_bw', 0))) + '/s'
        )
        fw_df['Site Storage %']  = parsed.apply(
            lambda x: f"{float(x.get('site_storage_util', 0)) * 100:.1f}%"
        )
        fw_df['Size (Human)']    = fw_df['Size'].apply(bytes_to_human)
        display_cols = ['Site', 'Host', 'File', 'Size (Human)', 'Disk Write BW',
                        'Disk', 'Site Storage %']
        st.dataframe(fw_df[display_cols], hide_index=True, use_container_width=True)
    else:
        st.info(f"No FileWrite events at timestep {selected_timestep:.4f}s")

# ─── Live Update Loop ─────────────────────────────────────────────────────────

should_exit_loop   = False
navigate_to_page   = None
last_selected_timestep = st.session_state.get('selected_timestep')

while not should_exit_loop:
    time.sleep(global_sleep_time)
    update_timestep_metric(output_db_path)

    # Rerun when the timestep slider/buttons change value
    current_selected_timestep = st.session_state.get('selected_timestep')
    if current_selected_timestep != last_selected_timestep:
        st.rerun()

    # Check for cell click (runs in both live and paused modes)
    for key in list(st.session_state.keys()):
        if key.startswith('bw_heatmap_'):
            widget_state = st.session_state[key]
            if hasattr(widget_state, 'selection') and widget_state.selection and widget_state.selection.points:
                for point in widget_state.selection.points:
                    cd = point.get('customdata')
                    if cd and len(cd) >= 2:
                        src_site, dst_site = cd[0], cd[1]
                        if src_site in all_sites and dst_site in all_sites:
                            st.session_state.s2s_source = src_site
                            st.session_state.s2s_dest   = dst_site
                            del st.session_state[key]
                            navigate_to_page = 'pages/6_Site_to_Site_Bandwidth.py'
                            should_exit_loop = True
                            break
        if should_exit_loop:
            break

    if should_exit_loop:
        continue

    if is_live_mode():
        try:
            new_df = get_transfer_data(output_db_path)
            new_hash = hash(str(new_df))
            if new_hash != last_df_hash:
                last_df_hash = new_hash
                if all_sites:
                    render_metrics(new_df)
                    z_new, c_new = build_heatmap_matrix(new_df, all_sites)
                    fig_new = build_heatmap_fig(z_new, c_new, all_sites)
                    heatmap_container.plotly_chart(
                        fig_new,
                        config={"width": "stretch"},
                        on_select="rerun",
                        selection_mode="points",
                        key=f"bw_heatmap_{st.session_state.bw_heatmap_counter}"
                    )
                    st.session_state.bw_heatmap_counter += 1
                status_container.success(f"Last updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            status_container.error(f"Error updating heatmap: {e}")
            time.sleep(1)

# Navigate after the while loop exits cleanly
if navigate_to_page:
    st.switch_page(navigate_to_page)

