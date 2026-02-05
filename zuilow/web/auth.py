"""
ZuiLow auth: users from config/users.yaml (role=admin), Flask-Login.

Load users from config/users.yaml; only accounts with role=admin. Flask-Login for session;
X-Webhook-Token header for server-to-server (e.g. PPT webhook). Ref: sai/ppt/core/auth.py.

Classes:
    User   UserMixin; id, username, role

Functions:
    init_login_manager(app: Flask) -> None   Setup Flask-Login; load_user callback
    authenticate(username: str, password: str) -> Optional[User]
    login_required_api(f)   Decorator: require current_user.is_authenticated or valid X-Webhook-Token

Auth config:
    config/users.yaml: list of {username, password_hash, role}; only role=admin allowed
    WEBHOOK_TOKEN from config/brokers/ppt.yaml for X-Webhook-Token check

"""
import os
from functools import wraps
from pathlib import Path

from flask import jsonify, request
from flask_login import LoginManager, UserMixin, current_user
from werkzeug.security import check_password_hash
import yaml


def _check_webhook_token() -> bool:
    """Check request for valid X-Webhook-Token (server-to-server)."""
    from .app import WEBHOOK_TOKEN
    if not WEBHOOK_TOKEN:
        return False
    req_token = request.headers.get("X-Webhook-Token", "")
    return req_token == WEBHOOK_TOKEN


# ============================================================
# User model
# ============================================================

class User(UserMixin):
    def __init__(self, username: str, password_hash: str, role: str):
        self.id = username
        self.username = username
        self.password_hash = password_hash
        self.role = role

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


# ============================================================
# User store (load admin only)
# ============================================================

_users: dict[str, User] = {}


def load_users(config_path: str | None = None) -> None:
    """Load users from YAML; only role=admin accounts are loaded."""
    global _users

    if config_path is None:
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config" / "users.yaml"

    if not os.path.exists(config_path):
        print(f"[Auth] Users config not found: {config_path}")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    users_config = config.get("users", {})
    _users = {}

    for username, user_data in users_config.items():
        role = (user_data.get("role") or "viewer").lower()
        if role != "admin":
            continue  # Admin-only for now
        _users[username] = User(
            username=username,
            password_hash=user_data.get("password", ""),
            role=role,
        )

    print(f"[Auth] Loaded {len(_users)} admin user(s): {list(_users.keys())}")


def get_user(username: str) -> User | None:
    return _users.get(username)


def authenticate(username: str, password: str) -> User | None:
    user = get_user(username)
    if user and user.check_password(password):
        return user
    return None


# ============================================================
# Flask-Login init
# ============================================================

login_manager = LoginManager()


def init_login_manager(app):
    login_manager.init_app(app)
    login_manager.login_view = "login_page"
    login_manager.login_message = "Please log in first"

    load_users()

    @login_manager.user_loader
    def load_user(user_id):
        return get_user(user_id)


# ============================================================
# Decorators
# ============================================================

def login_required_api(f):
    """Require login for API; supports session or X-Webhook-Token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if _check_webhook_token():
            return f(*args, **kwargs)
        if not current_user or not current_user.is_authenticated:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated
