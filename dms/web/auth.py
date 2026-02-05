"""
DMS auth: load users from config/users.yaml (role=admin); Flask-Login for session.

Used for: protecting web UI and optional API routes; only accounts with role=admin are loaded.

Classes:
    User   UserMixin; id, username, role

Functions:
    init_login_manager(app)   Setup Flask-Login; load_user callback
    authenticate(username, password) -> Optional[User]
    login_required_api(f)   Decorator: require current_user.is_authenticated
"""

import os
from functools import wraps
from pathlib import Path

from flask import jsonify
from flask_login import LoginManager, UserMixin, current_user
from werkzeug.security import check_password_hash
import yaml


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
            continue
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
    """Require login for API."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user or not current_user.is_authenticated:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated
