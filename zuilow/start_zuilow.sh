#!/bin/bash
# ZuiLow Web Service Startup Script (Flask, independent module)

cd "$(dirname "$0")"

# Use a single .pycache directory under project root
export PYTHONPYCACHEPREFIX="$(pwd)/.pycache"

# Add parent directory to PYTHONPATH so 'zuilow' can be imported as a package
export PYTHONPATH="$(pwd)/..:$PYTHONPATH"

#export DMS_API_KEY="your-shared-secret"
#export WEBHOOK_TOKEN="sim_webhook_shared_token"

# Activate conda environment
if [ -f ~/anaconda3/etc/profile.d/conda.sh ]; then
    source ~/anaconda3/etc/profile.d/conda.sh
    conda activate trade311
else
    echo "Warning: conda not found, trying to run Python directly"
fi

# Runtime data under ./run/ (db, logs, etc.)
mkdir -p run/db run/logs

echo "Starting ZuiLow Web Service..."
echo "Access URL: http://0.0.0.0:11180"
echo "Futu Panel: http://0.0.0.0:11180/futu"
echo "Scheduler: http://0.0.0.0:11180/scheduler"
echo ""

# Start Flask
python app.py
