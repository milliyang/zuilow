"""
Paper Trade 核心模块

- db: 数据库操作
- analytics: 绩效分析
- simulation: 交易模拟
- utils: 工具函数 (行情获取、代码转换)
- auth: 用户认证
"""

from . import db
from . import analytics
from . import simulation
from . import utils
from . import auth

__all__ = ['db', 'analytics', 'simulation', 'utils', 'auth']
