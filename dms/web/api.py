"""
DMS HTTP API: REST Blueprint for DMS; prefix /api/dms; requires global DMS instance set via set_dms_instance.

Used for: web UI and server-to-server calls (e.g. zuilow /api/market/quote); auth via session or X-API-Key.

Functions:
    set_dms_instance(instance)   Set global DMS instance (called by app startup)

Endpoints (all under /api/dms):
    GET  /status                  Node status (role, running, uptime, tasks_count)
    GET  /nodes                  All nodes status (master + slaves)
    GET  /sync/status             Sync status
    GET  /sync/history            Sync history (query: backup_name, limit, offset)
    POST /read/batch              Batch read (body: symbols, start_date, end_date, interval)
    GET  /read/<symbol>           Read single symbol (query: start_date, end_date, interval)
    GET  /symbols                 All symbols from tasks (cached; query: ttl_seconds optional)
    GET  /symbol/<symbol>/info   Symbol latest date and record count
    GET  /symbol/<symbol>/data   Symbol data (query: start_date, end_date, interval)
    POST /tasks/trigger           Trigger one task (body: task_name)
    POST /tasks/trigger-all       Trigger all tasks (body: task_type optional)
    GET  /tasks                   Task list
    GET  /tasks/<task_name>/status  Task status
    POST /sync/trigger            Trigger sync (body: backup_name optional)
    GET  /maintenance/log         Maintenance log (query: task_name, limit, offset)
    GET  /slaves                  Slave list
    GET  /slaves/<name>/status    Slave status
    POST /slaves/<name>/sync      Sync to slave (body: symbols, interval optional)
    POST /sync/request            Slave: request sync from master
    GET  /master/status           Slave: master status
    POST /export                  Export symbols to ZIP (body: symbols, interval, start_date, end_date)
    GET  /export/symbol/<symbol>  Export single symbol CSV (query: interval, start_date, end_date)
    GET  /exports                 List export files
    GET  /exports/<filename>      Download export file
    DELETE /exports/<filename>    Delete export file
    POST /database/clear          Clear primary database (dangerous)
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from flask import Blueprint, jsonify, request, abort, send_file
import pandas as pd

from flask_login import current_user

logger = logging.getLogger(__name__)

bp = Blueprint('dms_api', __name__)

# Server-to-server auth: X-API-Key header (e.g. zuilow calling DMS). Set DMS_API_KEY in env.
DMS_API_KEY = os.getenv("DMS_API_KEY", "").strip()


def _check_api_key() -> bool:
    """Allow request if X-API-Key matches DMS_API_KEY (server-to-server)."""
    if not DMS_API_KEY:
        return False
    key = request.headers.get("X-API-Key", "").strip()
    return key == DMS_API_KEY


@bp.before_request
def require_login():
    """Require login or valid X-API-Key for all DMS API routes."""
    if _check_api_key():
        return None  # allow request
    if not current_user.is_authenticated:
        return jsonify({"error": "Unauthorized"}), 401

# Global instances (will be set by app)
_dms_instance = None


def set_dms_instance(instance):
    """Set global DMS instance"""
    global _dms_instance
    _dms_instance = instance


# ============================================================================
# Common API (Master and Slave)
# ============================================================================

@bp.route("/status", methods=["GET"])
def get_status():
    """Get current node status"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    return jsonify({
        "role": _dms_instance.role,
        "running": _dms_instance.is_running,
        "uptime": _dms_instance.get_uptime(),
        "tasks_count": len(_dms_instance.get_tasks()) if hasattr(_dms_instance, "get_tasks") else 0,
    })


@bp.route("/nodes", methods=["GET"])
def get_all_nodes():
    """Get all nodes status (master + all slaves)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    return jsonify(_dms_instance.get_all_nodes_status())


@bp.route("/sync/status", methods=["GET"])
def get_sync_status():
    """Get sync status"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    return jsonify(_dms_instance.get_sync_status())


@bp.route("/sync/history", methods=["GET"])
def get_sync_history():
    """Get sync history"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    backup_name = request.args.get("backup_name", type=str)
    limit = request.args.get("limit", default=100, type=int)
    offset = request.args.get("offset", default=0, type=int)
    
    return jsonify(_dms_instance.get_sync_history(backup_name, limit, offset))


@bp.route("/read/batch", methods=["POST"])
def read_batch():
    """Batch read data (for backtesting)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        data = request.get_json(silent=True)
        if not data:
            abort(400, description="Request body is required")
        
        symbols = data.get("symbols", [])
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        interval = data.get("interval", "1d")
        as_of_str = data.get("as_of")  # optional: cap data at sim time (ISO datetime)
        
        if not symbols or not start_date_str or not end_date_str:
            abort(400, description="symbols, start_date, and end_date are required")
        
        start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if as_of_str:
            as_of = datetime.fromisoformat(as_of_str.replace("Z", "+00:00"))
            end_date = min(end_date, as_of)
        
        results = _dms_instance.read_batch(symbols, start_date, end_date, interval)
        
        # Convert DataFrames to JSON
        result_dict = {}
        for symbol, df in results.items():
            if df is not None:
                result_dict[symbol] = {
                    "data": df.to_dict("records"),
                    "index": [str(idx) for idx in df.index],
                }
            else:
                result_dict[symbol] = None
        
        return jsonify(result_dict)
        
    except Exception as e:
        logger.error(f"Error reading batch data: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/symbols", methods=["GET"])
def get_symbols():
    """
    Get all symbols (from task config). Cached in-memory for fast repeated calls.
    Query: ttl_seconds (optional) override cache TTL.
    Returns: {"symbols": ["US.AAPL", ...]}
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    try:
        ttl = request.args.get("ttl_seconds", type=int)
        symbols = _dms_instance.get_all_symbols_cached(ttl_seconds=ttl)
        return jsonify({"symbols": symbols})
    except Exception as e:
        logger.error("Error getting symbols: %s", e, exc_info=True)
        abort(500, description=str(e))


@bp.route("/read/<symbol>", methods=["GET"])
def read_symbol(symbol):
    """Read single symbol data"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        interval = request.args.get("interval", default="1d", type=str)
        
        if not start_date_str or not end_date_str:
            abort(400, description="start_date and end_date are required")
        
        start = datetime.fromisoformat(start_date_str)
        end = datetime.fromisoformat(end_date_str)
        
        data = _dms_instance.read_history(symbol, start, end, interval)
        
        if data is None:
            return jsonify({"symbol": symbol, "data": None})
        
        return jsonify({
            "symbol": symbol,
            "data": data.to_dict("records"),
            "index": [str(idx) for idx in data.index],
        })
        
    except Exception as e:
        logger.error(f"Error reading data for {symbol}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/symbol/<symbol>/info", methods=["GET"])
def get_symbol_info(symbol):
    """
    Get symbol data information (latest date, earliest date, data count)
    
    Args:
        symbol: Stock symbol
        interval: Time interval (default: "1d")
    
    Returns:
        Symbol information including latest date, earliest date, and data count
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        interval = request.args.get("interval", default="1d", type=str)
        
        # Get latest date from writer
        latest_date = _dms_instance.writer.get_latest_date(symbol, interval)
        
        # Get earliest date and count by reading a large range
        # We'll read from a very early date to now to get all data
        earliest_date = None
        data_count = 0
        
        # Always try to read data, even if latest_date is None
        # (data might exist but get_latest_date might have issues)
        from datetime import timedelta
        start_date = datetime.now() - timedelta(days=3650)  # 10 years
        end_date = datetime.now()
        
        data = _dms_instance.read_history(symbol, start_date, end_date, interval)
        
        if data is not None and len(data) > 0:
            data_count = len(data)
            earliest_date = data.index[0].to_pydatetime()
            # If latest_date was None but we have data, use the last data point
            if latest_date is None:
                latest_date = data.index[-1].to_pydatetime()
        
        return jsonify({
            "symbol": symbol,
            "interval": interval,
            "latest_date": latest_date.isoformat() if latest_date else None,
            "earliest_date": earliest_date.isoformat() if earliest_date else None,
            "data_count": data_count,
            "has_data": data_count > 0,
        })
        
    except Exception as e:
        logger.error(f"Error getting symbol info for {symbol}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/symbol/<symbol>/data", methods=["GET"])
def get_symbol_data(symbol):
    """
    Get paginated symbol data
    
    Args:
        symbol: Stock symbol
        interval: Time interval (default: "1d")
        page: Page number (1-based, default: 1)
        page_size: Number of records per page (default: 50)
        order: Sort order - "desc" for newest first, "asc" for oldest first (default: "desc")
    
    Returns:
        Paginated data with records and pagination info
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        interval = request.args.get("interval", default="1d", type=str)
        page = request.args.get("page", default=1, type=int)
        page_size = request.args.get("page_size", default=50, type=int)
        order = request.args.get("order", default="desc", type=str)
        
        from datetime import timedelta
        
        # Read all data
        start_date = datetime.now() - timedelta(days=3650)  # 10 years
        end_date = datetime.now()
        
        data = _dms_instance.read_history(symbol, start_date, end_date, interval)
        
        if data is None or len(data) == 0:
            return jsonify({
                "symbol": symbol,
                "interval": interval,
                "data": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
            })
        
        # Sort by time (descending by default - newest first)
        if order == "asc":
            data = data.sort_index(ascending=True)
        else:
            data = data.sort_index(ascending=False)
        
        # Calculate pagination
        total = len(data)
        total_pages = (total + page_size - 1) // page_size
        
        # Validate page number
        if page < 1:
            page = 1
        elif page > total_pages and total_pages > 0:
            page = total_pages
        
        # Get page data
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_data = data.iloc[start_idx:end_idx]
        
        # Convert to list of records
        records = []
        for timestamp, row in page_data.iterrows():
            records.append({
                "time": timestamp.isoformat(),
                "Open": float(row["Open"]),
                "High": float(row["High"]),
                "Low": float(row["Low"]),
                "Close": float(row["Close"]),
                "Volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
            })
        
        return jsonify({
            "symbol": symbol,
            "interval": interval,
            "data": records,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "order": order,
        })
        
    except Exception as e:
        logger.error(f"Error getting symbol data for {symbol}: {e}", exc_info=True)
        abort(500, description=str(e))


# ============================================================================
# Master Node API
# ============================================================================

@bp.route("/tasks/trigger", methods=["POST"])
def trigger_task():
    """Manually trigger a task (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can trigger tasks")
    
    # Try to get task_name from query parameter first, then from JSON body
    task_name = request.args.get("task_name")
    if not task_name:
        # Use silent=True to avoid exception when request body is empty or invalid JSON
        data = request.get_json(silent=True) or {}
        task_name = data.get("task_name")
    
    if not task_name:
        abort(400, description="task_name is required (as query parameter or in JSON body)")
    
    try:
        result = _dms_instance.trigger_task(task_name)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.error(f"Error triggering task {task_name}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/tasks/trigger-all", methods=["POST"])
def trigger_all_tasks():
    """
    Trigger all tasks (master only)
    
    Query params:
        task_type: Optional task type filter (e.g., "incremental", "full_sync")
                  If None, trigger all tasks
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can trigger tasks")
    
    try:
        task_type = request.args.get("task_type", type=str)
        result = _dms_instance.trigger_all_tasks(task_type)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.error(f"Error triggering all tasks: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/tasks", methods=["GET"])
def list_tasks():
    """Get task list (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can list tasks")
    
    return jsonify(_dms_instance.get_tasks())


@bp.route("/tasks/<task_name>/status", methods=["GET"])
def get_task_status(task_name):
    """Get task status (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can get task status")
    
    return jsonify(_dms_instance.get_task_status(task_name))


@bp.route("/sync/trigger", methods=["POST"])
def trigger_sync():
    """Manually trigger sync to backup nodes (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can trigger sync")
    
    try:
        backup_name = request.args.get("backup_name", type=str)
        result = _dms_instance.trigger_sync(backup_name)
        return jsonify({"success": True, "message": "Sync triggered", "result": result})
    except Exception as e:
        logger.error(f"Error triggering sync: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/maintenance/log", methods=["GET"])
def get_maintenance_log():
    """Get maintenance log (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can get maintenance log")
    
    try:
        task_name = request.args.get("task_name", type=str)
        limit = request.args.get("limit", default=100, type=int)
        offset = request.args.get("offset", default=0, type=int)
        
        logs = _dms_instance.get_maintenance_log(
            task_name=task_name,
            limit=limit,
            offset=offset,
        )
        return jsonify({"logs": logs})
    except Exception as e:
        logger.error(f"Error getting maintenance log: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/slaves", methods=["GET"])
def list_slaves():
    """Get slave nodes list (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can list slaves")
    
    return jsonify(_dms_instance.get_slaves())


@bp.route("/slaves/<slave_name>/status", methods=["GET"])
def get_slave_status(slave_name):
    """Get slave node status (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can get slave status")
    
    return jsonify(_dms_instance.get_slave_status(slave_name))


@bp.route("/slaves/<slave_name>/sync", methods=["POST"])
def sync_to_slave(slave_name):
    """Sync data to specific slave node (master only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can sync to slaves")
    
    try:
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol")
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        
        start_date = None
        end_date = None
        
        if start_date_str:
            start_date = datetime.fromisoformat(start_date_str)
        if end_date_str:
            end_date = datetime.fromisoformat(end_date_str)
        
        result = _dms_instance.sync_to_slave(slave_name, symbol, start_date, end_date)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.error(f"Error syncing to slave {slave_name}: {e}", exc_info=True)
        abort(500, description=str(e))


# ============================================================================
# Slave Node API
# ============================================================================

@bp.route("/sync/request", methods=["POST"])
def request_sync():
    """Request master node to sync data to local (slave only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "slave":
        abort(403, description="Only slave node can request sync")
    
    try:
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol")
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        
        start_date = datetime.fromisoformat(start_date_str) if start_date_str else None
        end_date = datetime.fromisoformat(end_date_str) if end_date_str else None
        
        result = _dms_instance.request_sync_from_master(symbol, start_date, end_date)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.error(f"Error requesting sync: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/master/status", methods=["GET"])
def get_master_status():
    """Get master node status (slave only)"""
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "slave":
        abort(403, description="Only slave node can get master status")
    
    return jsonify(_dms_instance.get_master_status())


# ============================================================================
# Export API (Common)
# ============================================================================

@bp.route("/export", methods=["POST"])
def export_data():
    """
    Export data to CSV files and optionally create ZIP archive
    
    Body:
        - symbols: List of symbols to export (use ["*"] or ["all"] to export all symbols)
        - interval: Time interval (default: "1d")
        - start_date: Start date (optional, format: YYYY-MM-DD)
        - end_date: End date (optional, format: YYYY-MM-DD)
        - create_zip: Whether to create ZIP archive (default: true)
    
    Returns:
        Export result with file paths
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        data = request.get_json(silent=True) or {}
        if not data:
            abort(400, description="Request body is required")
        
        # Handle export all symbols
        symbols = data.get("symbols", [])
        if not symbols or symbols == ["*"] or symbols == ["all"]:
            symbols = _dms_instance.get_all_symbols()
            if not symbols:
                abort(400, description="No symbols found in configuration")
            logger.info(f"Exporting all {len(symbols)} symbols")
        
        interval = data.get("interval", "1d")
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        create_zip = data.get("create_zip", True)
        
        # Parse dates
        start_date = datetime.fromisoformat(start_date_str) if start_date_str else None
        end_date = datetime.fromisoformat(end_date_str) if end_date_str else None
        
        # Export
        result = _dms_instance.exporter.export_all_symbols(
            symbols=symbols,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            create_zip=create_zip,
        )
        
        return jsonify({"success": True, "result": result})
        
    except Exception as e:
        logger.error(f"Error exporting data: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/export/symbol/<symbol>", methods=["GET"])
def export_single_symbol(symbol):
    """
    Export single symbol to CSV
    
    Args:
        symbol: Stock symbol
        interval: Time interval (default: "1d")
    
    Returns:
        Export result with file path
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        interval = request.args.get("interval", default="1d", type=str)
        filepath = _dms_instance.exporter.export_symbol(symbol, interval)
        
        if filepath:
            return jsonify({
                "success": True,
                "filepath": filepath,
                "filename": filepath.split("/")[-1],
            })
        else:
            abort(404, description=f"No data found for {symbol}")
            
    except Exception as e:
        logger.error(f"Error exporting {symbol}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/exports", methods=["GET"])
def list_exports():
    """
    List all export files
    
    Returns:
        List of export files with metadata
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        files = _dms_instance.exporter.list_exports()
        return jsonify({"success": True, "files": files})
    except Exception as e:
        logger.error(f"Error listing exports: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/exports/<filename>", methods=["GET"])
def download_export(filename):
    """
    Download export file
    
    Args:
        filename: File name to download
    
    Returns:
        File response
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        filepath = _dms_instance.exporter.get_export_path(filename)
        
        if filepath is None:
            abort(404, description="File not found")
        
        return send_file(
            str(filepath),
            as_attachment=True,
            download_name=filename,
        )
        
    except Exception as e:
        logger.error(f"Error downloading {filename}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/exports/<filename>", methods=["DELETE"])
def delete_export(filename):
    """
    Delete export file
    
    Args:
        filename: File name to delete
    
    Returns:
        Success status
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    try:
        success = _dms_instance.exporter.delete_export(filename)
        
        if success:
            return jsonify({"success": True, "message": f"Deleted {filename}"})
        else:
            abort(404, description="File not found")
            
    except Exception as e:
        logger.error(f"Error deleting {filename}: {e}", exc_info=True)
        abort(500, description=str(e))


@bp.route("/database/clear", methods=["POST"])
def clear_database():
    """
    Clear all data from database (DESTRUCTIVE OPERATION!)
    
    WARNING: This will delete all data in the database!
    
    Query params:
        confirm: Must be true to proceed (safety check)
    
    Returns:
        Result with success status
    """
    if _dms_instance is None:
        abort(503, description="DMS not initialized")
    
    if _dms_instance.role != "master":
        abort(403, description="Only master node can clear database")
    
    confirm = request.args.get("confirm", default=False, type=bool)
    if not confirm:
        abort(400, description="Must provide confirm=true query parameter to proceed with this destructive operation")
    
    try:
        result = _dms_instance.clear_database()
        return jsonify({"success": result["success"], "message": result["message"]})
    except Exception as e:
        logger.error(f"Error clearing database: {e}", exc_info=True)
        abort(500, description=str(e))
