#!/bin/bash

# CGSim Visualization - Installation Script
# This script creates a Python virtual environment and installs dependencies

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== CGSim Visualization Installation ==="
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi

PYTHON_VERSION=$(python3 --version)
echo "Found $PYTHON_VERSION"

# Check if venv module is available
if ! python3 -c "import venv" &> /dev/null; then
    echo "Error: Python venv module is not installed."
    echo "Please install it with one of the following commands:"
    echo "  - Ubuntu/Debian: sudo apt install python3-venv"
    echo "  - Fedora/RHEL: sudo dnf install python3-venv"
    echo "  - macOS: venv is included with Python 3 from python.org or Homebrew"
    exit 1
fi

echo "Python venv module is available."
echo ""

# Create virtual environment
if [ -d ".venv" ]; then
    echo "Virtual environment .venv already exists."
    read -p "Do you want to recreate it? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing .venv..."
        rm -rf .venv
        echo "Creating new virtual environment..."
        python3 -m venv .venv
    fi
else
    echo "Creating virtual environment in .venv..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo ""
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo ""
echo "=== Installation Complete ==="
echo ""
echo "To run the visualization app, use:"
echo "  ./run_visualization.sh"
echo ""
echo "Or manually activate the environment and run:"
echo "  source .venv/bin/activate"
echo "  streamlit run CGSim_Visualization.py"

