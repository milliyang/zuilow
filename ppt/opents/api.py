"""
OpenTimestamps API: Blueprint for timestamp history, detail, record/proof download, create, verify.

Used for: PPT web UI and admin; all routes under /api/ots; login_required or admin_required.

Endpoints:
    GET  /api/ots/history         Timestamp history (limit query)
    GET  /api/ots/detail/<date>   Detail for one date
    GET  /api/ots/record/<date>   Download record file
    GET  /api/ots/proof/<date>    Download proof file
    POST /api/ots/create          Create timestamp (admin)
    POST /api/ots/verify/<date>   Verify timestamp (admin)
"""
import os
import sys
import logging
from pathlib import Path
from flask import Blueprint, jsonify, request, send_file, Response, current_app
from core.auth import admin_required, login_required_api

from . import service

# 配置日志
logger = logging.getLogger(__name__)

# 模块加载时输出信息（仅输出一次）
if not hasattr(service, '_module_loaded'):
    print(f"[OTS API] 模块已加载, OTS_AVAILABLE={service.OTS_AVAILABLE}", flush=True)
    service._module_loaded = True

bp = Blueprint('opentimestamps', __name__)


@bp.route('/api/ots/history', methods=['GET'])
@login_required_api
def get_history():
    """获取时间戳历史记录"""
    limit = int(request.args.get('limit', 100))
    history = service.get_timestamp_history(limit)
    return jsonify({
        'history': history,
        'total': len(history)
    })


@bp.route('/api/ots/detail/<date>', methods=['GET'])
@login_required_api
def get_detail(date: str):
    """
    Get detail for one timestamp date.
    
    格式：/api/ots/detail/2026-01-27_16-00-00 或 /api/ots/detail/2026-01-27_label
    """
    detail = service.get_timestamp_detail(date)
    if not detail:
        return jsonify({'error': f'时间戳 {date} 不存在'}), 404
    
    # 简化返回（不返回完整记录数据，避免响应过大）
    return jsonify({
        'date': detail['date'],
        'time_suffix': detail.get('time_suffix'),
        'label': detail.get('label'),
        'timestamp': detail['record'].get('timestamp'),
        'next_trading_day': detail['record'].get('next_trading_day'),
        'record_file': detail['record_file'],
        'proof_file': detail['proof_file'],
        'has_proof': detail['has_proof'],
        'file_hash': detail['file_hash'],
        'summary': detail['record'].get('summary', {}),
        'verification': detail.get('verification'),
    })


@bp.route('/api/ots/record/<date>', methods=['GET'])
@login_required_api
def download_record(date: str):
    """
    下载原始记录文件
    
    格式：/api/ots/record/2026-01-27_16-00-00 或 /api/ots/record/2026-01-27_label
    """
    record_file = service.RECORDS_DIR / f"record_{date}.json"
    
    if not record_file.exists():
        return jsonify({'error': f'时间戳 {date} 的记录文件不存在'}), 404
    
    return send_file(
        str(record_file),
        mimetype='application/json',
        as_attachment=True,
        download_name=record_file.name
    )


@bp.route('/api/ots/proof/<date>', methods=['GET'])
@login_required_api
def download_proof(date: str):
    """
    下载证明文件
    
    格式：/api/ots/proof/2026-01-27_16-00-00 或 /api/ots/proof/2026-01-27_label
    """
    proof_file = service.PROOFS_DIR / f"record_{date}.ots"
    
    if not proof_file.exists():
        return jsonify({'error': f'时间戳 {date} 的证明文件不存在'}), 404
    
    return send_file(
        str(proof_file),
        mimetype='application/octet-stream',
        as_attachment=True,
        download_name=proof_file.name
    )


def _create_timestamp_impl():
    """创建时间戳的实际实现"""
    logger.info("[OTS API] /api/ots/create 被调用")
    print(f"[OTS API] /api/ots/create 被调用, OTS_AVAILABLE={service.OTS_AVAILABLE}", flush=True)
    
    try:
        result = service.create_daily_timestamp()
        logger.info(f"[OTS API] create_daily_timestamp() 返回: success={result.get('success')}")
        
        if result.get('success'):
            return jsonify({
                'status': 'ok',
                'message': '时间戳创建成功',
                **result
            })
        else:
            return jsonify({
                'status': 'error',
                'error': result.get('error', '未知错误'),
                **result
            }), 500
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_trace = traceback.format_exc()
        
        # 使用多种方式输出错误，确保能看到
        error_output = f"[OTS API] ❌ 创建时间戳异常: {error_msg}\n错误堆栈:\n{error_trace}"
        logger.exception("[OTS API] 创建时间戳异常")
        print(error_output, file=sys.stderr, flush=True)
        print(error_output, flush=True)
        sys.stderr.flush()
        sys.stdout.flush()
        traceback.print_exc()
        
        return jsonify({
            'status': 'error',
            'error': f'服务器错误: {error_msg}',
            'traceback': error_trace if os.getenv('FLASK_DEBUG') == '1' else None
        }), 500


@bp.route('/api/ots/create', methods=['POST'])
@admin_required
def create_timestamp():
    """手动创建时间戳 (admin)"""
    return _create_timestamp_impl()


@bp.route('/api/ots/verify/<date>', methods=['POST'])
@admin_required
def verify_timestamp(date: str):
    """
    验证时间戳 (admin)
    
    格式：/api/ots/verify/2026-01-27_16-00-00 或 /api/ots/verify/2026-01-27_label
    """
    record_file = service.RECORDS_DIR / f"record_{date}.json"
    
    if not record_file.exists():
        return jsonify({'error': f'时间戳 {date} 不存在'}), 404
    
    proof_file = service.PROOFS_DIR / f"{record_file.stem}.ots"
    
    if not proof_file.exists():
        return jsonify({'error': '证明文件不存在'}), 404
    
    verify_result = service.verify_proof(record_file, proof_file)
    
    return jsonify({
        'date': date,
        'verification': verify_result,
        'file_hash': service.calculate_file_hash(record_file),
    })


@bp.route('/api/ots/info', methods=['GET'])
@login_required_api
def get_info():
    """获取OpenTimestamps服务信息"""
    import os
    history = service.get_timestamp_history(limit=360)  # 获取最近5条记录
    latest = history[0] if history else None
    
    # 从环境变量获取配置信息
    schedule = os.getenv('OTS_TIMESTAMP_SCHEDULE', '16:0')
    github_enabled = os.getenv('OTS_AUTO_GITHUB', 'false').lower() == 'true'
    
    # 解析定时任务配置，显示更友好的信息
    schedule_display = schedule
    if schedule and schedule.lower() != 'off':
        items = [item.strip() for item in schedule.split(',')]
        if len(items) > 1:
            schedule_display = f"{len(items)} 个时间点: {schedule}"
    
    return jsonify({
        'service': 'OpenTimestamps',
        'storage_dir': str(service.STORAGE_DIR),
        'records_dir': str(service.RECORDS_DIR),
        'proofs_dir': str(service.PROOFS_DIR),
        'schedule': schedule_display if schedule.lower() != 'off' else '已禁用',
        'github_enabled': github_enabled,
        'latest_timestamp': latest,
        'recent_history': history[:5],  # 最近5条记录
        'calendar_servers': service.OTS_CALENDAR_SERVERS,
    })
