# -*- coding: utf-8 -*-
"""
Qlib-Pro v16 配置文件
所有策略阈值集中管理，便于回测调参
"""

import os
import json
from pathlib import Path


def _env_str(name: str, default: str) -> str:
    """读取字符串环境变量"""
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    """读取整数环境变量"""
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    """读取浮点环境变量"""
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    """读取布尔环境变量"""
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_local_config() -> dict:
    """读取项目根目录下的本地配置文件。"""
    config_path = Path(__file__).with_name("config.local.json")
    if not config_path.exists():
        return {}

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


LOCAL_CONFIG = _load_local_config()


def _local_str(name: str, default: str) -> str:
    value = LOCAL_CONFIG.get(name)
    return str(value).strip() if value not in (None, "") else default


def _local_bool(name: str, default: bool) -> bool:
    value = LOCAL_CONFIG.get(name)
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

# ==================== 性能参数 ====================
MAX_IO_WORKERS = _env_int("MOMENTUM_MAX_IO_WORKERS", 15)   # 网络IO并发数
MAX_AI_WORKERS = _env_int("MOMENTUM_MAX_AI_WORKERS", 3)    # NLP模型并发数

# ==================== 回测配置 ====================
SLIPPAGE = _env_float("MOMENTUM_SLIPPAGE", 0.008)             # 交易滑点 (0.8%)
POOL_SIZE = _env_int("MOMENTUM_POOL_SIZE", 150)               # 回测底池大小
BACKTEST_DAYS_DEFAULT = _env_int("MOMENTUM_BACKTEST_DAYS_DEFAULT", 250)  # 默认回测天数
HOLD_PERIOD_DEFAULT = _env_int("MOMENTUM_HOLD_PERIOD_DEFAULT", 5)        # 默认持仓周期
NLP_SCORE_DEFAULT = 0.15     # 默认NLP得分 (无数据时)
INITIAL_CAPITAL = 100000.0   # 初始资金 (10万元)

# ==================== 风控阈值 ====================
RSI_DANGER_ZONE = 80.0       # 极度超买熔断位
VOL_SURGE_LIMIT = 3.0        # 量比惩罚起始位
ATR_STOP_FACTOR = 1.2        # ATR动态止损系数 - 120天优化: 1.2
FIXED_STOP_PCT = 0.05        # 固定止损百分比 - 120天优化: 5%最优
TAKE_PROFIT_PCT = 0.10       # 固定止盈百分比 - 120天优化: 10%最优
BIAS_PROFIT_LIMIT = 0.20     # 乖离率止盈阈值
USE_ADAPTIVE_EXIT = True     # 自适应止损 - 120天优化: True夏普1.72 vs False 0.34

# ==================== 选股阈值 ====================
MIN_CHANGE_PCT = 4.0         # 最小涨幅%
MAX_CHANGE_PCT = 9.2         # 最大涨幅%
MIN_VOL_RATIO = 1.2          # 最小量比
MIN_AMOUNT = _env_int("MOMENTUM_MIN_AMOUNT", 200000000)       # 最小成交额 (2亿)
MIN_SHARPE = 1.0             # 最小夏普比率
MAX_SECTOR_PICKS = 1         # 同行业最大持仓数 - 120天优化: 1最优
MAX_TOTAL_PICKS = _env_int("MOMENTUM_MAX_TOTAL_PICKS", 3)     # 总选股数量上限

# ==================== 庄股识别配置 ====================
ENABLE_MANIPULATION_FILTER = False   # 是否启用庄股过滤 (120天验证无差异，关闭)
MANIPULATION_SCORE_THRESHOLD = 50    # 庄股评分阈值 (0-100, 超过则过滤)
MOMENTUM_R2_THRESHOLD = 0.90         # R²阈值 (走势过于光滑)
IVOL_PERCENTILE = 0.95               # 特质波动率百分位阈值
ILLIQ_PERCENTILE = 0.95              # 非流动性百分位阈值
OVERNIGHT_RATIO_THRESHOLD = 0.75     # 隔夜收益占比阈值

# ==================== 市场宽度 ====================
MARKET_BREADTH_DEFENSE = 0.22  # 空仓防御阈值
MARKET_AMOUNT_LOW = 800000000000   # 缩量市阈值 (8000亿)
MARKET_AMOUNT_HIGH = 2000000000000 # 爆量市阈值 (2万亿)

# ==================== NLP权重 ====================
ENABLE_NLP_ANALYSIS = True       # 是否启用 Ollama NLP 分析
OLLAMA_MODEL = "qwen2.5:3b"      # Ollama 模型名称 (原: qwen3:8b, 3b速度更快)
NLP_CANDIDATE_SIZE = 10          # NLP分析候选数量 (优化: 先量化筛选Top N再NLP)
NLP_WEIGHT_LOW_VOL = 0.5     # 缩量市NLP权重
NLP_WEIGHT_HIGH_VOL = 0.2    # 爆量市NLP权重
NLP_WEIGHT_NORMAL = 0.3      # 常规市NLP权重

# ==================== 连板高度择时 ====================
STREAK_EMOTION_HIGH = 5      # 情绪高点 (减仓信号)
STREAK_EMOTION_LOW = 2       # 情绪冰点 (加仓信号)

# ==================== 因子权重 ====================
FACTOR_WEIGHTS = {
    'mom_5': 0.30,
    'mom_20': 0.10,
    'sharpe': 0.25,
    'chip_rate': -0.15,      # 负权重 (股东减少为正)
    'big_order': 0.20,
    'divergence': 0.50,
}

# ==================== 数据库配置 ====================
DB_PATH = _env_str("MOMENTUM_DB_PATH", "qlib_pro_v16.db")
KLINE_START_DATE = '2024-06-01'  # K线缓存起始日期

# ==================== 网络配置 ====================
NETWORK_TIMEOUT = 10             # 网络请求超时秒数
NETWORK_MAX_RETRIES = 3          # 网络最大重试次数

# ==================== 日志配置 ====================
LOG_FILE = _env_str("MOMENTUM_LOG_FILE", "qlib_pro.log")
LOG_LEVEL = _env_str("MOMENTUM_LOG_LEVEL", "INFO")   # DEBUG / INFO / WARNING / ERROR
# ==================== 飞书通知配置 ====================
FEISHU_WEBHOOK_URL = _env_str(
    "FEISHU_WEBHOOK_URL",
    _local_str("FEISHU_WEBHOOK_URL", ""),
).strip()
ENABLE_FEISHU_NOTIFICATION = _env_bool(
    "MOMENTUM_ENABLE_FEISHU_NOTIFICATION",
    _local_bool("MOMENTUM_ENABLE_FEISHU_NOTIFICATION", bool(FEISHU_WEBHOOK_URL)),
)

# ==================== Gemini AI 配置 ====================
ENABLE_GEMINI_ADVICE = False         # 是否启用 Gemini AI 建议
GEMINI_MODEL = "gemini-2.5-flash"   # 使用的模型 (免费层级可用: gemini-2.0-flash, gemini-1.5-flash)
GEMINI_TIMEOUT = 30                 # 请求超时秒数

# ==================== ETF 行业轮动配置 ====================
ETF_MIN_AMOUNT = 50000000           # ETF 最低成交额 (5000万)
ETF_MAX_PICKS_PER_TYPE = 2          # 每类最多选几只
ETF_MAX_TOTAL_PICKS = 5             # 总共最多选几只
ETF_RSI_UPPER = 75                  # RSI 超买惩罚阈值
ETF_RSI_LOWER = 30                  # RSI 超卖加分阈值

# ETF 类型分类关键词
ETF_TYPE_KEYWORDS = {
    '行业': ['芯片', '半导体', 'AI', '人工智能', '软件', '通信', '5G', '云计算',
             '新能源', '光伏', '锂电', '储能', '风电', '军工', '航天', '国防',
             '医药', '医疗', '创新药', '生物', '消费', '白酒', '食品', '家电',
             '汽车', '银行', '证券', '保险', '金融', '地产', '基建', '建材',
             '有色', '煤炭', '钢铁', '化工', '农业', '养殖', '传媒', '游戏'],
    '宽基': ['沪深300', '中证500', '中证1000', '上证50', '创业板', '科创板',
             '上证180', '深证100', 'A50', '红利', '价值', '成长'],
    '商品': ['黄金', '白银', '原油', '豆粕', '有色金属', '能源化工'],
    '跨境': ['纳斯达克', '纳指', '标普', 'S&P', '恒生', '港股', '日经',
             '德国', '法国', '英国', '越南', '印度', '东南亚'],
    '债券': ['国债', '企业债', '信用债', '可转债', '利率债'],
    '货币': ['货币', '现金', '理财'],
}

# ETF 因子权重
ETF_FACTOR_WEIGHTS = {
    'trend_momentum': 0.25,      # 趋势动量 (20日涨幅)
    'fund_flow': 0.25,           # 资金流强度
    'relative_strength': 0.20,   # 相对强弱 (vs 沪深300)
    'risk_adjusted': 0.15,       # 风险调整收益
    'ma_position': 0.15,         # 均线位置
}
