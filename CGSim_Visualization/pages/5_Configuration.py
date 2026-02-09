import streamlit as st
import os
import json

st.set_page_config(layout="wide",
                   page_title="CGSim - Configuration",
                   page_icon="📊")

# Path to the config file (in the current working directory where streamlit is run)
CONFIG_FILE_PATH = "vis_config.json"

# Default configuration values
DEFAULT_CONFIG = {
    "output_dir": "../output",
    "output_filename": "output.db",
    "config_dir": "../config-files",
    "selected_config_file": "site_info.json"
}


def load_config():
    """Load configuration from vis_config.json file."""
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            with open(CONFIG_FILE_PATH, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config():
    """Save current configuration to vis_config.json file."""
    config = {
        "output_dir": st.session_state.output_dir,
        "output_filename": st.session_state.output_filename,
        "config_dir": st.session_state.config_dir,
        "selected_config_file": st.session_state.selected_config_file
    }
    try:
        with open(CONFIG_FILE_PATH, 'w') as f:
            json.dump(config, f, indent=4)
        return True
    except IOError as e:
        st.error(f"IOError: {e}")
        return False


st.write("# CGSim Configuration")
st.write("Configure the paths for your simulation output database and site configuration files.")

# Load config from file on first run of the entire app
if "config_loaded" not in st.session_state:
    saved_config = load_config()
    st.session_state.output_dir = saved_config.get("output_dir", DEFAULT_CONFIG["output_dir"])
    st.session_state.output_filename = saved_config.get("output_filename", DEFAULT_CONFIG["output_filename"])
    st.session_state.config_dir = saved_config.get("config_dir", DEFAULT_CONFIG["config_dir"])
    st.session_state.selected_config_file = saved_config.get("selected_config_file", DEFAULT_CONFIG["selected_config_file"])
    # Also set site_info_json_path from loaded config
    st.session_state.site_info_json_path = os.path.join(
        st.session_state.config_dir,
        st.session_state.selected_config_file
    )
    st.session_state.config_loaded = True

# Initialize session state with defaults if not already set
if "output_dir" not in st.session_state:
    st.session_state.output_dir = DEFAULT_CONFIG["output_dir"]

if "output_filename" not in st.session_state:
    st.session_state.output_filename = DEFAULT_CONFIG["output_filename"]

# Always recalculate output_db_path from output_dir and output_filename
st.session_state.output_db_path = os.path.join(st.session_state.output_dir, st.session_state.output_filename)

if "config_dir" not in st.session_state:
    st.session_state.config_dir = DEFAULT_CONFIG["config_dir"]

if "site_info_json_path" not in st.session_state:
    st.session_state.site_info_json_path = os.path.join(DEFAULT_CONFIG["config_dir"], DEFAULT_CONFIG["selected_config_file"])

if "config_files_list" not in st.session_state:
    st.session_state.config_files_list = []

if "selected_config_file" not in st.session_state:
    st.session_state.selected_config_file = DEFAULT_CONFIG["selected_config_file"]


def scan_config_directory():
    """Scan the config directory and update the list of available config files."""
    config_dir = st.session_state.config_dir
    if os.path.exists(config_dir) and os.path.isdir(config_dir):
        # Get all JSON files in the directory
        files = [f for f in os.listdir(config_dir) if f.endswith('.json')]
        st.session_state.config_files_list = sorted(files)
        # If current selection is not in the list, reset to first file
        if st.session_state.selected_config_file not in files:
            st.session_state.selected_config_file = files[0] if files else None
    else:
        st.session_state.config_files_list = []
        st.session_state.selected_config_file = None


def update_output_db_path():
    """Update the full output database path from directory and filename."""
    st.session_state.output_db_path = os.path.join(
        st.session_state.output_dir,
        st.session_state.output_filename
    )


def on_output_dir_change():
    """Callback when output directory changes."""
    update_output_db_path()


def on_output_filename_change():
    """Callback when output filename changes."""
    update_output_db_path()


def on_config_dir_change():
    """Callback when config directory changes - scans the directory."""
    scan_config_directory()


def on_config_file_select():
    """Callback when a config file is selected from the dropdown."""
    if st.session_state.selected_config_file:
        st.session_state.site_info_json_path = os.path.join(
            st.session_state.config_dir,
            st.session_state.selected_config_file
        )


# === Output Database Configuration ===
st.write("---")
st.write("## Output Database Configuration")

col1, col2 = st.columns([2, 1])

with col1:
    st.text_input(
        "Output Directory",
        key="output_dir",
        on_change=on_output_dir_change,
        help="Path to the directory containing the simulation output database. Press Enter to apply."
    )

with col2:
    st.text_input(
        "Output Filename",
        key="output_filename",
        on_change=on_output_filename_change,
        help="Name of the database file (default: output.db). Press Enter to apply."
    )

# Show current full path and status
full_db_path = st.session_state.output_db_path
if os.path.exists(full_db_path):
    st.code(f"Database found: `{full_db_path}`")
else:
    st.warning(f"Database not found: `{full_db_path}`")


# === Config Files Configuration ===
st.write("---")
st.write("## Site Configuration Files")

st.text_input(
    "Config Files Directory",
    key="config_dir",
    on_change=on_config_dir_change,
    help="Path to the directory containing site configuration JSON files. Press Enter to scan the directory."
)

# Scan directory on first load or if list is empty
if not st.session_state.config_files_list:
    scan_config_directory()

# Show directory status and dropdown
config_dir = st.session_state.config_dir
if os.path.exists(config_dir) and os.path.isdir(config_dir):
    if st.session_state.config_files_list:
        st.code(f"Found {len(st.session_state.config_files_list)} config file(s) in `{config_dir}`")

        # Ensure selected_config_file is valid before rendering selectbox
        if st.session_state.selected_config_file not in st.session_state.config_files_list:
            st.session_state.selected_config_file = st.session_state.config_files_list[0]

        # Calculate the index for the selectbox
        current_index = st.session_state.config_files_list.index(st.session_state.selected_config_file)

        selected = st.selectbox(
            "Select Site Configuration File",
            st.session_state.config_files_list,
            index=current_index,
            on_change=on_config_file_select,
            help="Choose which site configuration file to use for the visualization."
        )

        # Update session state with the selected value (since we're not using key=)
        st.session_state.selected_config_file = selected

        # Update the path
        on_config_file_select()

        st.info(f"Current config file: `{st.session_state.site_info_json_path}`")
    else:
        st.warning(f"No JSON files found in `{config_dir}`")
else:
    st.error(f"Directory not found: `{config_dir}`")


# === Current Configuration Summary ===
st.write("---")
st.write("## Configuration Summary")

summary_col1, summary_col2 = st.columns(2)

with summary_col1:
    st.write("**Output Database Path:**")
    st.code(st.session_state.output_db_path)

with summary_col2:
    st.write("**Site Configuration Path:**")
    st.code(st.session_state.site_info_json_path)


# === Save Configuration ===
st.write("---")
if st.button("Save Configuration", use_container_width=True, type="primary"):
    if save_config():
        st.success("Configuration saved successfully!")
    else:
        st.error("Failed to save configuration.")


# === Navigation ===
st.write("---")
if st.button("← Back to Overview", use_container_width=True):
    st.switch_page('CGSim_Visualization.py')