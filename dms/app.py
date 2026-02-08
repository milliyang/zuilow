"""
DMS Flask application: standalone HTTP service; loads config, creates DMS, registers API and static files.

Used for: running DMS as a web service (default port 11183).

Entry:
    python -m dms.app or python app.py: startup() loads config, creates DMS(config), dms.start(), set_dms_instance(dms); then app.run(host, port). Port from config.service.port (default 11183).

Features:
    - Adds project root to sys.path; loads .env via dotenv
    - Registers Blueprint from dms.web.api at /api/dms; serves static from web/static; GET / serves index.html
"""

import sys
import logging
import os
from pathlib import Path

# Add parent directory to Python path so 'dms' can be imported as a package
# This allows relative imports like 'from ..sources' to work
_project_root = Path(__file__).parent
_parent_dir = _project_root.parent
if str(_parent_dir) not in sys.path:
    sys.path.insert(0, str(_parent_dir))

from flask import Flask, request, jsonify, redirect, send_from_directory
from flask_cors import CORS
from flask_login import login_user, logout_user, current_user, login_required
from dotenv import load_dotenv

from dms.web.api import bp as api_bp, set_dms_instance
from dms.web.auth import init_login_manager, authenticate
from dms.core.config import load_config

# Load environment variables
load_dotenv()

# Reduce Werkzeug access log verbosity
# Werkzeug uses its own logger named 'werkzeug' (not 'dms')
# Note: Errors will still be logged (WARNING level), but normal requests won't
logging.getLogger('werkzeug').setLevel(logging.INFO)

# Application logger
logger = logging.getLogger(__name__)

# Global DMS instance
_dms_instance = None

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY') or os.urandom(24).hex()
# Different cookie name so zuilow and ppt can run in same browser without logging each other out
app.config['SESSION_COOKIE_NAME'] = 'dms_session'

# Initialize CORS
CORS(app, supports_credentials=True)

# Auth (admin only, ref zuilow)
init_login_manager(app)

# Register API Blueprint
app.register_blueprint(api_bp, url_prefix='/api/dms')

# Static files
static_dir = Path(__file__).parent / "web" / "static"
if static_dir.exists():
    app.static_folder = str(static_dir)
    app.static_url_path = '/static'


@app.route("/login")
def login_page():
    """Login page"""
    if current_user.is_authenticated:
        return redirect("/")
    return send_from_directory(static_dir, "login.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    """Login API"""
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password", "")
    user = authenticate(username, password)
    if user:
        login_user(user)
        return jsonify({"status": "ok", "user": {"username": user.username, "role": user.role}})
    return jsonify({"error": "Invalid username or password"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    """Logout API"""
    logout_user()
    return jsonify({"status": "ok"})


@app.route("/api/user")
def api_user():
    """Current user (for frontend)"""
    if current_user.is_authenticated:
        return jsonify({
            "authenticated": True,
            "username": current_user.username,
            "role": current_user.role,
        })
    return jsonify({"authenticated": False})


@app.route("/")
@login_required
def index():
    """Serve main page (login required)"""
    index_file = static_dir / "index.html"
    if index_file.exists():
        with open(index_file, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>DMS Service</h1><p>Web interface not available</p>"


# Initialize DMS on startup (before first request)
def startup():
    """Initialize DMS on startup"""
    global _dms_instance
    try:
        logger.info("Starting DMS Service...")
        
        # Load configuration
        from dms.core.config import load_config
        config = load_config()
        
        # Initialize DMS instance (this will trigger database health check)
        from dms.core.dms import DMS
        dms = DMS(config)
        dms.start()
        _dms_instance = dms
        set_dms_instance(dms)
        
        logger.info("DMS service started successfully")
    except Exception as e:
        logger.error(f"ERROR: Failed to start DMS service: {e}", exc_info=True)
        raise  # Re-raise to prevent service from starting with errors

# Call startup immediately
startup()


if __name__ == "__main__":
    host = os.getenv("DMS_HOST", "0.0.0.0")
    try:
        port_env = os.getenv("DMS_PORT")
        if port_env is not None:
            port = int(port_env)
        else:
            from dms.core.config import load_config
            config = load_config()
            port = config.service.port
    except Exception:
        port = int(os.getenv("DMS_PORT", "11183"))
    
    app.run(host=host, port=port, debug=False)
