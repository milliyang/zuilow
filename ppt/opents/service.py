"""
OpenTimestamps service: collect account/trade/equity data, generate record, submit to OTS calendar, save record and proof.

Used for: opents API create/verify; record and proof files under OTS_STORAGE_DIR (default run/opentimestamps).

Functions:
    get_timestamp_history(limit) -> List[Dict]
    get_timestamp_detail(date) -> Optional[Dict]
    create_timestamp(...) -> Dict   Create record, submit to calendar, save proof
    verify_timestamp(date) -> Dict  Verify proof and return result

Features:
    - Record: JSON with accounts, equity curves, trades; proof: binary from opentimestamps library
    - Requires opentimestamps Python library; OTS_AVAILABLE False if not installed
"""
import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from core import db as database
from core.analytics import get_full_analytics
from core.utils import get_quotes_batch, get_equity_date, get_current_datetime_iso, is_sim_mode

# 配置日志
logger = logging.getLogger(__name__)

# 尝试导入 opentimestamps Python 库
try:
    from opentimestamps.core.timestamp import Timestamp, DetachedTimestampFile
    from opentimestamps.core.op import OpSHA256
    from opentimestamps.calendar import RemoteCalendar
    from opentimestamps.core.serialize import BytesSerializationContext
    OTS_AVAILABLE = True
    logger.info("[OTS] opentimestamps Python 库已加载")
except ImportError as e:
    OTS_AVAILABLE = False
    logger.error(f"[OTS] 警告: opentimestamps Python 库未安装: {e}")
    logger.error("[OTS] 安装: pip install opentimestamps")


# 存储目录
STORAGE_DIR = Path(os.getenv('OTS_STORAGE_DIR', 'run/opentimestamps'))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# 原始记录目录
RECORDS_DIR = STORAGE_DIR / 'records'
RECORDS_DIR.mkdir(parents=True, exist_ok=True)

# 证明文件目录
PROOFS_DIR = STORAGE_DIR / 'proofs'
PROOFS_DIR.mkdir(parents=True, exist_ok=True)

# OpenTimestamps 日历服务器
OTS_CALENDAR_SERVERS = [
    'https://alice.btc.calendar.opentimestamps.org',
    'https://bob.btc.calendar.opentimestamps.org',
    'https://finney.calendar.eternitywall.com',
]


def get_next_trading_day(date: datetime = None) -> str:
    """
    获取下一个交易日（简化版：跳过周末）
    
    Args:
        date: 基准日期，默认今天
    
    Returns:
        下一个交易日的日期字符串 (YYYY-MM-DD)
    """
    if date is None:
        date = get_equity_date()
    
    # 简单实现：跳过周末
    next_day = date + timedelta(days=1)
    while next_day.weekday() >= 5:  # 5=Saturday, 6=Sunday
        next_day += timedelta(days=1)
    
    return next_day.strftime('%Y-%m-%d')


def collect_account_data(account_name: str, quotes: Dict = None) -> Dict[str, Any]:
    """
    收集单个账户的所有数据
    
    Returns:
        包含账户所有信息的字典
    """
    account = database.get_account(account_name)
    if not account:
        return {}
    
    positions = database.get_positions(account_name)
    orders = database.get_orders(account_name, limit=10000)
    trades = database.get_trades(account_name, limit=10000)
    equity_history = database.get_equity_history(account_name)
    
    # 获取绩效分析
    analytics = get_full_analytics(account_name, quotes)
    
    return {
        'account': {
            'name': account_name,
            'initial_capital': account['initial_capital'],
            'cash': account['cash'],
            'created_at': account['created_at'],
        },
        'positions': positions,
        'orders': orders,
        'trades': trades,
        'equity_history': equity_history,
        'analytics': analytics,
        'collected_at': get_current_datetime_iso(),
    }


def collect_all_accounts_data() -> Dict[str, Any]:
    """
    收集所有账户的数据，生成原始记录
    
    Returns:
        包含所有账户数据的字典
    """
    accounts = database.get_all_accounts()
    
    # 获取所有持仓的实时行情（用于计算准确市值）
    all_symbols = set()
    for acc in accounts:
        positions = database.get_positions(acc['name'])
        all_symbols.update(positions.keys())
    
    quotes = get_quotes_batch(list(all_symbols)) if all_symbols else {}
    
    # 收集所有账户数据
    accounts_data = {}
    for acc in accounts:
        accounts_data[acc['name']] = collect_account_data(acc['name'], quotes)
    
    # 生成完整记录
    record = {
        'date': get_equity_date().strftime('%Y-%m-%d'),
        'timestamp': get_current_datetime_iso(),
        'next_trading_day': get_next_trading_day(),
        'accounts': accounts_data,
        'summary': {
            'total_accounts': len(accounts),
            'total_positions': sum(len(acc.get('positions', {})) for acc in accounts_data.values()),
            'total_trades': sum(len(acc.get('trades', [])) for acc in accounts_data.values()),
        }
    }
    
    return record


def generate_record_file(record: Dict[str, Any], label: str = None) -> Path:
    """
    将原始记录保存为JSON文件
    
    Args:
        record: 记录数据
        label: 可选的标签（用于区分不同的任务，如 "us_market", "hk_market"）
               如果不提供，使用时间戳中的时分秒
    
    Returns:
        保存的文件路径
    """
    date = record['date']
    timestamp_str = record.get('timestamp', get_current_datetime_iso())
    
    # 将标签保存到记录中
    if label:
        record['label'] = label
    
    # 从时间戳中提取时分秒，格式：2026-01-27T22:47:22.633751 -> 22-47-22
    if label:
        # 使用提供的标签作为文件名后缀
        time_suffix = label
    else:
        # 从时间戳中提取时分秒
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            time_suffix = dt.strftime('%H-%M-%S')
        except (ValueError, TypeError):
            time_suffix = datetime.fromisoformat(get_current_datetime_iso().replace('Z', '+00:00')).strftime('%H-%M-%S')
    
    filename = f"record_{date}_{time_suffix}.json"
    filepath = RECORDS_DIR / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    
    return filepath


def calculate_file_hash(filepath: Path) -> str:
    """
    计算文件的SHA256哈希值
    
    Returns:
        十六进制哈希字符串
    """
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()




def submit_to_opentimestamps(filepath: Path) -> Optional[Path]:
    """
    提交文件到OpenTimestamps获取时间戳证明
    
    Args:
        filepath: 要提交的文件路径
    
    Returns:
        证明文件路径 (.ots)，如果失败返回None
    """
    
    if not OTS_AVAILABLE:
        error_msg = "opentimestamps Python 库未安装，请运行: pip install opentimestamps"
        logger.error(f"[OTS] ❌ {error_msg}")
        return None
    
    # 生成证明文件路径
    proof_filepath = PROOFS_DIR / f"{filepath.stem}.ots"
    
    try:
        logger.info("[OTS] 使用 Python API 创建时间戳...")
        logger.info(f"[OTS] 文件路径: {filepath}")
        
        # 读取文件并计算哈希
        logger.info("[OTS] 读取文件并计算哈希...")
        with open(filepath, 'rb') as f:
            file_hash_op = OpSHA256()
            detached_ts = DetachedTimestampFile.from_fd(file_hash_op, f)
        
        logger.info(f"[OTS] 文件哈希: {detached_ts.file_digest.hex()}")
        
        # 提交到远程日历服务器
        timestamp = detached_ts.timestamp
        
        # 尝试多个日历服务器
        calendars = [
            RemoteCalendar('https://alice.btc.calendar.opentimestamps.org'),
            RemoteCalendar('https://bob.btc.calendar.opentimestamps.org'),
            RemoteCalendar('https://finney.calendar.eternitywall.com'),
        ]
        
        submitted = False
        for calendar in calendars:
            try:
                logger.info(f"[OTS] 提交到 {calendar.url}...")
                calendar_timestamp = calendar.submit(timestamp.msg, timeout=30)
                timestamp.merge(calendar_timestamp)
                submitted = True
                logger.info(f"[OTS] ✓ 成功提交到 {calendar.url}")
                break
            except Exception as e:
                logger.warning(f"[OTS] 提交到 {calendar.url} 失败: {type(e).__name__}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                continue
        
        if not submitted:
            logger.error("[OTS] 所有日历服务器提交失败")
            return None
        
        # 更新 detached timestamp
        logger.info("[OTS] 更新时间戳对象...")
        detached_ts = DetachedTimestampFile(file_hash_op, timestamp)
        
        # 保存证明文件
        logger.info("[OTS] 序列化并保存证明文件...")
        ctx = BytesSerializationContext()
        detached_ts.serialize(ctx)
        proof_bytes = ctx.getbytes()
        logger.info(f"[OTS] 证明文件大小: {len(proof_bytes)} 字节")
        
        with open(proof_filepath, 'wb') as f:
            f.write(proof_bytes)
        
        logger.info(f"[OTS] 时间戳提交成功: {proof_filepath}")
        return proof_filepath
            
    except Exception as e:
        logger.error(f"[OTS] 时间戳提交异常: {type(e).__name__}: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def verify_proof(record_file: Path, proof_file: Path) -> Dict[str, Any]:
    """
    验证时间戳证明
    
    Returns:
        验证结果字典
    """
    if not OTS_AVAILABLE:
        return {
            'verified': False,
            'output': None,
            'error': 'opentimestamps Python 库未安装'
        }
    
    try:
        from opentimestamps.core.serialize import BytesDeserializationContext
        
        # 读取证明文件
        with open(proof_file, 'rb') as f:
            ctx = BytesDeserializationContext(f.read())
            detached_ts = DetachedTimestampFile.deserialize(ctx)
        
        # 读取原始文件并计算哈希
        with open(record_file, 'rb') as f:
            file_hash_op = OpSHA256()
            file_hash = file_hash_op.hash_fd(f)
        
        # 验证文件哈希是否匹配
        if detached_ts.file_digest != file_hash:
            return {
                'verified': False,
                'output': None,
                'error': '文件哈希不匹配'
            }
        
        # 检查是否有时间证明
        attestations = list(detached_ts.timestamp.all_attestations())
        if not attestations:
            return {
                'verified': False,
                'output': '时间戳已创建，但尚未在区块链上确认',
                'error': None
            }
        
        # 返回验证结果（注意：完整验证需要本地 Bitcoin Core 节点）
        return {
            'verified': True,
            'output': f'时间戳已创建，包含 {len(attestations)} 个证明',
            'error': None
        }
            
    except Exception as e:
        return {
            'verified': False,
            'output': None,
            'error': str(e)
        }


def create_daily_timestamp(label: str = None) -> Dict[str, Any]:
    """
    创建每日时间戳（主函数）
    
    Args:
        label: 可选的标签（用于区分不同的任务，如 "us_market", "hk_market"）
               如果不提供，使用时间戳中的时分秒
    
    流程:
    1. 收集所有账户数据
    2. 生成原始记录文件
    3. 提交到OpenTimestamps
    4. 保存证明文件
    
    Returns:
        操作结果字典
    """
    logger.info(f"[OTS] 开始创建每日时间戳: {get_current_datetime_iso()}, label={label}")
    
    try:
        # 1. 收集数据
        logger.info("[OTS] 正在收集账户数据...")
        record = collect_all_accounts_data()
        
        # 如果有标签，保存到记录中
        if label:
            record['label'] = label
        
        # 2. 保存原始记录
        logger.info("[OTS] 正在保存原始记录...")
        record_file = generate_record_file(record, label=label)
        file_hash = calculate_file_hash(record_file)
        logger.info(f"[OTS] 原始记录已保存: {record_file}")
        logger.info(f"[OTS] 文件哈希: {file_hash}")
        
        # 3. 提交到OpenTimestamps
        logger.info("[OTS] 正在提交到OpenTimestamps...")
        proof_file = submit_to_opentimestamps(record_file)
        
        if not proof_file:
            return {
                'success': False,
                'error': '时间戳提交失败',
                'record_file': str(record_file),
                'proof_file': None,
                'file_hash': file_hash,
            }
        
        # 4. 验证证明（可选，可能需要等待区块链确认）
        logger.info("[OTS] 验证时间戳证明...")
        verify_result = verify_proof(record_file, proof_file)
        
        # 从文件名中提取时间后缀
        time_suffix = record_file.stem.replace(f"record_{record['date']}_", "")
        
        result = {
            'success': True,
            'date': record['date'],
            'timestamp': record['timestamp'],
            'time_suffix': time_suffix,
            'label': label,  # 添加标签信息
            'next_trading_day': record['next_trading_day'],
            'record_file': str(record_file),
            'proof_file': str(proof_file),
            'file_hash': file_hash,
            'verification': verify_result,
        }
        
        # 可选：自动提交到 GitHub（仿真模式下不调用 GitHub）
        if is_sim_mode():
            result['github'] = {'success': False, 'skipped': True, 'reason': 'simulation mode'}
        else:
            try:
                from . import github
                github_result = github.auto_commit_after_timestamp(result)
                result['github'] = github_result
                if github_result.get('success'):
                    logger.info(f"[OTS] 已自动提交到 GitHub: {github_result.get('repo')}")
            except Exception as e:
                logger.warning(f"[OTS] GitHub 自动提交失败: {e}")
                result['github'] = {'success': False, 'error': str(e)}
        
        logger.info(f"[OTS] 时间戳创建成功: {result['date']}")
        return result
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"[OTS] 创建时间戳失败: {e}")
        logger.error(f"[OTS] 错误堆栈:\n{error_trace}")
        return {
            'success': False,
            'error': str(e),
        }


def get_timestamp_history(limit: int = 100) -> List[Dict[str, Any]]:
    """
    获取时间戳历史记录
    
    Returns:
        历史记录列表（按时间戳倒序排列）
    """
    history = []
    
    # 读取所有记录文件
    for record_file in sorted(RECORDS_DIR.glob('record_*.json'), reverse=True):
        if len(history) >= limit:
            break
        
        try:
            # 解析文件名：record_2026-01-27_16-00-00.json 或 record_2026-01-27.json
            stem = record_file.stem  # record_2026-01-27_16-00-00 或 record_2026-01-27
            parts = stem.replace('record_', '').split('_')
            date = parts[0]  # 2026-01-27
            time_suffix = '_'.join(parts[1:]) if len(parts) > 1 else None  # 16-00-00 或 None
            
            proof_file = PROOFS_DIR / f"{record_file.stem}.ots"
            
            # 读取记录文件获取元数据
            with open(record_file, 'r', encoding='utf-8') as f:
                record_data = json.load(f)
            
            history.append({
                'date': date,
                'time_suffix': time_suffix,  # 时间后缀（如 16-00-00 或 us_market）
                'timestamp': record_data.get('timestamp'),
                'label': record_data.get('label'),  # 从记录中获取标签（如果有）
                'next_trading_day': record_data.get('next_trading_day'),
                'record_file': str(record_file),
                'proof_file': str(proof_file) if proof_file.exists() else None,
                'has_proof': proof_file.exists(),
                'summary': record_data.get('summary', {}),
            })
        except Exception as e:
            logger.error(f"[OTS] 读取历史记录失败 {record_file}: {e}")
    
    # 按时间戳倒序排列（最新的在前）
    history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    
    return history


def get_timestamp_detail(date: str) -> Optional[Dict[str, Any]]:
    """
    获取指定日期的详细时间戳信息
    
    Args:
        date: 完整文件名标识符 (YYYY-MM-DD_HH-MM-SS 或 YYYY-MM-DD_label)
    
    Returns:
        详细信息字典，如果不存在返回None
    """
    record_file = RECORDS_DIR / f"record_{date}.json"
    
    if not record_file.exists():
        return None
    
    return get_timestamp_detail_by_file(record_file)


def get_timestamp_detail_by_file(record_file: Path) -> Optional[Dict[str, Any]]:
    """
    根据文件路径获取详细时间戳信息
    
    Args:
        record_file: 记录文件路径
    
    Returns:
        详细信息字典，如果不存在返回None
    """
    if not record_file.exists():
        return None
    
    try:
        # 解析文件名
        stem = record_file.stem
        parts = stem.replace('record_', '').split('_')
        date = parts[0]
        time_suffix = '_'.join(parts[1:]) if len(parts) > 1 else None
        
        with open(record_file, 'r', encoding='utf-8') as f:
            record_data = json.load(f)
        
        proof_file = PROOFS_DIR / f"{record_file.stem}.ots"
        file_hash = calculate_file_hash(record_file)
        
        result = {
            'date': date,
            'time_suffix': time_suffix,
            'label': record_data.get('label'),
            'record': record_data,
            'record_file': str(record_file),
            'proof_file': str(proof_file) if proof_file.exists() else None,
            'has_proof': proof_file.exists(),
            'file_hash': file_hash,
        }
        
        # 如果证明文件存在，尝试验证
        if proof_file.exists():
            verify_result = verify_proof(record_file, proof_file)
            result['verification'] = verify_result
        
        return result
        
    except Exception as e:
        logger.error(f"[OTS] 读取时间戳详情失败: {e}")
        return None
