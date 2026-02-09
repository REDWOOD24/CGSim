#!/bin/bash

# CGSim Visualization - Run Script
# This script activates the virtual environment and runs the Streamlit app

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found."
    echo "Please run ./install_visualization.sh first."
    exit 1
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Check if streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "Error: Streamlit is not installed in the virtual environment."
    echo "Please run ./install_visualization.sh to install dependencies."
    exit 1
fi

# Run the Streamlit app
echo "Starting CGSim Visualization..."
echo "The app will open in your default web browser."
echo "Press Ctrl+C to stop the server."
echo ""

streamlit run CGSim_Visualization.py

