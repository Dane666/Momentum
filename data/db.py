# -*- coding: utf-8 -*-
"""
数据库模块
- SQLite 初始化
- 因子日志存储
- 回测日志存储
- 回测交易记录存储 (用于可视化)
"""

import sqlite3
import pandas as pd
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, List, Dict
import logging
import uuid

logger = logging.getLogger('momentum')


def get_db_path():
    """获取数据库路径 (延迟导入避免循环依赖)"""
    from .. import config as cfg
    return cfg.DB_PATH


@contextmanager
def get_db_connection():
    """数据库连接上下文管理器"""
    conn = sqlite3.connect(get_db_path())
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def init_db():
    """初始化 SQLite 数据库表结构"""
    with get_db_connection() as conn:
        c = conn.cursor()

        # 因子记录表
        c.execute('''CREATE TABLE IF NOT EXISTS factor_logs (
            date TEXT,
            code TEXT,
            name TEXT,
            alpha_score REAL,
            nlp_score REAL,
            bias_20 REAL,
            divergence REAL,
            rsi REAL,
            close REAL,
            comment TEXT
        )''')

        # K线缓存表
        c.execute('''CREATE TABLE IF NOT EXISTS kline_cache (
            code TEXT,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            turnover_ratio REAL,
            PRIMARY KEY (code, trade_date)
        )''')

        # 股票板块缓存表 (避免重复网络请求)
        c.execute('''CREATE TABLE IF NOT EXISTS stock_sector_cache (
            code TEXT PRIMARY KEY,
            sector TEXT,
            update_time TEXT
        )''')

        # 回测日志表
        c.execute('''CREATE TABLE IF NOT EXISTS backtest_logs (
            date TEXT,
            code TEXT,
            alpha REAL,
            bias_20 REAL,
            atr REAL,
            fwd_ret REAL,
            exit_reason TEXT
        )''')

        # ============ 回测交易记录表 (用于可视化) ============
        # 回测会话表 - 记录每次回测的元信息
        c.execute('''CREATE TABLE IF NOT EXISTS backtest_sessions (
            session_id TEXT PRIMARY KEY,
            start_time TEXT,
            end_time TEXT,
            backtest_days INTEGER,
            hold_period INTEGER,
            pool_size INTEGER,
            slippage REAL,
            initial_capital REAL,
            final_nav REAL,
            total_return REAL,
            annual_return REAL,
            sharpe_ratio REAL,
            max_drawdown REAL,
            win_rate REAL,
            total_trades INTEGER,
            status TEXT DEFAULT 'running'
        )''')

        # 回测交易明细表 - 记录每笔买卖
        c.execute('''CREATE TABLE IF NOT EXISTS backtest_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            trade_id TEXT,
            code TEXT,
            name TEXT,
            sector TEXT,
            style_group TEXT,
            trade_type TEXT,
            trade_date TEXT,
            price REAL,
            shares INTEGER,
            amount REAL,
            commission REAL,
            alpha_score REAL,
            mom_5 REAL,
            mom_20 REAL,
            sharpe REAL,
            rsi REAL,
            bias_20 REAL,
            atr REAL,
            exit_reason TEXT,
            pnl REAL,
            pnl_pct REAL,
            hold_days INTEGER,
            action_label TEXT,
            position_value REAL,
            account_balance REAL,
            created_at TEXT,
            FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id)
        )''')

        # 回测每日净值表 - 用于绘制净值曲线
        c.execute('''CREATE TABLE IF NOT EXISTS backtest_equity_curve (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            trade_date TEXT,
            nav REAL,
            daily_return REAL,
            cumulative_return REAL,
            drawdown REAL,
            position_count INTEGER,
            benchmark_nav REAL,
            FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id)
        )''')

        # 回测持仓快照表 - 记录每日持仓状态
        c.execute('''CREATE TABLE IF NOT EXISTS backtest_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            trade_date TEXT,
            code TEXT,
            name TEXT,
            shares INTEGER,
            cost_price REAL,
            current_price REAL,
            market_value REAL,
            unrealized_pnl REAL,
            unrealized_pnl_pct REAL,
            hold_days INTEGER,
            FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id)
        )''')

        # 创建索引以提高查询性能
        c.execute('CREATE INDEX IF NOT EXISTS idx_trades_session ON backtest_trades(session_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_trades_date ON backtest_trades(trade_date)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_trades_code ON backtest_trades(code)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_equity_session ON backtest_equity_curve(session_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_positions_session ON backtest_positions(session_id)')

    logger.info(f"[DB] 数据库初始化完成: {get_db_path()}")


def save_factor_logs(df_picks: pd.DataFrame, date_str: str):
    """
    批量写入因子数据

    Args:
        df_picks: 选股结果 DataFrame
        date_str: 日期字符串
    """
    if df_picks.empty:
        return

    try:
        with get_db_connection() as conn:
            data = []
            for _, row in df_picks.iterrows():
                data.append((
                    date_str,
                    row['code'],
                    row['name'],
                    row.get('total_alpha', 0),
                    row.get('nlp_score', 0),
                    row.get('bias_20', 0),
                    row.get('divergence', 0),
                    row.get('rsi', 0),
                    row.get('close', 0),
                    row.get('action', '')
                ))

            conn.executemany(
                'INSERT INTO factor_logs VALUES (?,?,?,?,?,?,?,?,?,?)',
                data
            )
        logger.info(f"[DB] 已存档 {len(data)} 条选股记录")
    except Exception as e:
        logger.error(f"[DB] 写入失败: {e}")


def save_backtest_logs(logs: list):
    """
    保存回测日志

    Args:
        logs: 回测记录列表 [{'date': ..., 'code': ..., ...}, ...]
    """
    if not logs:
        return

    try:
        with get_db_connection() as conn:
            data = [(
                log['date'],
                log['code'],
                log.get('alpha', 0),
                log.get('bias_20', 0),
                log.get('atr', 0),
                log.get('fwd_ret', 0),
                log.get('exit_reason', '')
            ) for log in logs]

            conn.executemany(
                'INSERT INTO backtest_logs VALUES (?,?,?,?,?,?,?)',
                data
            )
        logger.info(f"[DB] 已保存 {len(data)} 条回测记录")
    except Exception as e:
        logger.error(f"[DB] 回测日志写入失败: {e}")


# ============ 回测交易记录器 ============

class BacktestTradeRecorder:
    """
    回测交易记录器
    
    用于记录回测过程中的所有买卖操作，支持后续可视化分析
    
    使用示例:
    ```python
    recorder = BacktestTradeRecorder(
        backtest_days=250,
        hold_period=3,
        initial_capital=1000000
    )
    
    # 记录买入
    recorder.record_buy(
        trade_date='2025-01-15',
        code='600519',
        name='贵州茅台',
        price=1800.0,
        shares=100,
        alpha_score=2.5,
        ...
    )
    
    # 记录卖出
    recorder.record_sell(
        trade_date='2025-01-18',
        code='600519',
        name='贵州茅台',
        price=1850.0,
        shares=100,
        exit_reason='MA5_Exit',
        ...
    )
    
    # 记录净值
    recorder.record_equity(trade_date='2025-01-15', nav=1.05, ...)
    
    # 完成回测
    recorder.finalize(final_nav=1.25, total_return=25.0, ...)
    ```
    """

    def __init__(
        self,
        backtest_days: int,
        hold_period: int,
        pool_size: int = 500,
        slippage: float = 0.002,
        initial_capital: float = 100000.0
    ):
        """
        初始化交易记录器
        
        Args:
            backtest_days: 回测天数
            hold_period: 持仓周期
            pool_size: 股票池大小
            slippage: 滑点
            initial_capital: 初始资金 (默认10万)
        """
        self.session_id = str(uuid.uuid4())[:12]
        self.backtest_days = backtest_days
        self.hold_period = hold_period
        self.pool_size = pool_size
        self.slippage = slippage
        self.initial_capital = initial_capital
        self.start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 账户资金跟踪
        self.account_balance = initial_capital  # 当前可用资金
        self.total_position_value = 0.0  # 当前持仓市值
        
        # 内存缓存 (批量写入提高性能)
        self._trades_buffer: List[Dict] = []
        self._equity_buffer: List[Dict] = []
        self._positions_buffer: List[Dict] = []
        self._trade_counter = 0
        
        # 持仓跟踪 (用于计算盈亏)
        self._open_positions: Dict[str, Dict] = {}
        
        # 创建会话记录
        self._create_session()
        
        logger.info(f"[TradeRecorder] 创建回测会话: {self.session_id}")

    def _create_session(self):
        """创建回测会话记录"""
        try:
            with get_db_connection() as conn:
                conn.execute('''
                    INSERT INTO backtest_sessions 
                    (session_id, start_time, backtest_days, hold_period, 
                     pool_size, slippage, initial_capital, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.session_id,
                    self.start_time,
                    self.backtest_days,
                    self.hold_period,
                    self.pool_size,
                    self.slippage,
                    self.initial_capital,
                    'running'
                ))
        except Exception as e:
            logger.error(f"[TradeRecorder] 创建会话失败: {e}")

    def record_buy(
        self,
        trade_date: str,
        code: str,
        name: str,
        price: float,
        shares: int = 100,
        sector: str = '',
        style_group: str = '',
        alpha_score: float = 0.0,
        alpha_trend: float = 0.0,
        mom_5: float = 0.0,
        mom_20: float = 0.0,
        sharpe: float = 0.0,
        rsi: float = 0.0,
        bias_20: float = 0.0,
        atr: float = 0.0,
        commission: float = 0.0,
        action_label: str = '',
        position_value: float = 0.0
    ):
        """
        记录买入交易
        
        Args:
            trade_date: 交易日期
            code: 股票代码
            name: 股票名称
            price: 买入价格
            shares: 买入股数
            sector: 所属板块
            style_group: 风格分组 (大/中/小盘)
            alpha_score: Alpha 分数
            alpha_trend: Alpha 趋势
            mom_5: 5日动量
            mom_20: 20日动量
            sharpe: 夏普比率
            rsi: RSI 指标
            bias_20: 20日乖离率
            atr: ATR 波动率
            commission: 手续费
            action_label: 操作标签 (如 🎯 [回测买入])
            position_value: 本笔仓位金额
        """
        self._trade_counter += 1
        trade_id = f"{self.session_id}_{self._trade_counter:06d}"
        amount = price * shares
        
        # 更新账户余额
        self.account_balance -= amount + commission
        self.total_position_value += amount
        
        trade_record = {
            'session_id': self.session_id,
            'trade_id': trade_id,
            'code': code,
            'name': name,
            'sector': sector,
            'style_group': style_group,
            'trade_type': 'BUY',
            'trade_date': trade_date,
            'price': price,
            'shares': shares,
            'amount': amount,
            'commission': commission,
            'alpha_score': alpha_score,
            'mom_5': mom_5,
            'mom_20': mom_20,
            'sharpe': sharpe,
            'rsi': rsi,
            'bias_20': bias_20,
            'atr': atr,
            'exit_reason': None,
            'pnl': None,
            'pnl_pct': None,
            'hold_days': None,
            'action_label': action_label,
            'position_value': position_value if position_value > 0 else amount,
            'account_balance': self.account_balance + self.total_position_value,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self._trades_buffer.append(trade_record)
        
        # 记录持仓
        self._open_positions[code] = {
            'trade_id': trade_id,
            'buy_date': trade_date,
            'buy_price': price,
            'shares': shares,
            'amount': amount,
            'name': name,
            'sector': sector,
            'style_group': style_group,
            'alpha_trend': alpha_trend
        }

    def record_sell(
        self,
        trade_date: str,
        code: str,
        name: str,
        price: float,
        shares: int = 100,
        exit_reason: str = 'Time_Exit',
        sector: str = '',
        style_group: str = '',
        alpha_score: float = 0.0,
        mom_5: float = 0.0,
        mom_20: float = 0.0,
        sharpe: float = 0.0,
        rsi: float = 0.0,
        bias_20: float = 0.0,
        atr: float = 0.0,
        commission: float = 0.0,
        override_hold_days: int = None
    ):
        """
        记录卖出交易
        
        Args:
            trade_date: 交易日期
            code: 股票代码
            name: 股票名称
            price: 卖出价格
            shares: 卖出股数
            exit_reason: 卖出原因 (ATR_Stop/MA5_Exit/Time_Exit)
            override_hold_days: 覆盖持仓天数（交易日，如果提供则使用它而不是自然日差值）
            其他参数同 record_buy
        """
        self._trade_counter += 1
        trade_id = f"{self.session_id}_{self._trade_counter:06d}"
        amount = price * shares
        
        # 计算盈亏
        pnl = None
        pnl_pct = None
        hold_days = None
        buy_amount = 0
        
        if code in self._open_positions:
            pos = self._open_positions[code]
            buy_price = pos['buy_price']
            buy_amount = pos.get('amount', buy_price * shares)
            pnl = (price - buy_price) * shares - commission
            pnl_pct = ((price / buy_price) - 1) * 100
            
            # 计算持仓天数: 优先使用传入的交易日天数，否则用自然日差值
            if override_hold_days is not None:
                hold_days = override_hold_days
            else:
                try:
                    buy_dt = datetime.strptime(pos['buy_date'], '%Y-%m-%d')
                    sell_dt = datetime.strptime(trade_date, '%Y-%m-%d')
                    hold_days = (sell_dt - buy_dt).days
                except:
                    hold_days = self.hold_period
            
            # 使用持仓信息补充字段
            if not sector:
                sector = pos.get('sector', '')
            if not style_group:
                style_group = pos.get('style_group', '')
            
            del self._open_positions[code]
        
        # 更新账户余额
        self.account_balance += amount - commission
        self.total_position_value -= buy_amount
        
        trade_record = {
            'session_id': self.session_id,
            'trade_id': trade_id,
            'code': code,
            'name': name,
            'sector': sector,
            'style_group': style_group,
            'trade_type': 'SELL',
            'trade_date': trade_date,
            'price': price,
            'shares': shares,
            'amount': amount,
            'commission': commission,
            'alpha_score': alpha_score,
            'mom_5': mom_5,
            'mom_20': mom_20,
            'sharpe': sharpe,
            'rsi': rsi,
            'bias_20': bias_20,
            'atr': atr,
            'exit_reason': exit_reason,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'hold_days': hold_days,
            'action_label': None,
            'position_value': amount,
            'account_balance': self.account_balance + self.total_position_value,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self._trades_buffer.append(trade_record)

    def record_trade_pair(
        self,
        buy_date: str,
        sell_date: str,
        code: str,
        name: str,
        buy_price: float,
        sell_price: float,
        shares: int = 100,
        exit_reason: str = 'Time_Exit',
        sector: str = '',
        style_group: str = '',
        alpha_score: float = 0.0,
        alpha_trend: float = 0.0,
        mom_5: float = 0.0,
        mom_20: float = 0.0,
        sharpe: float = 0.0,
        rsi: float = 0.0,
        bias_20: float = 0.0,
        atr: float = 0.0,
        actual_hold_days: int = None,
        action_label: str = '',
        position_value: float = 0.0
    ):
        """
        一次性记录买卖配对 (适用于回测场景)
        
        Args:
            buy_date: 买入日期
            sell_date: 卖出日期
            actual_hold_days: 实际持仓交易日天数（如果提供，则使用它而不是自然日差值）
            action_label: 操作标签 (如 🎯 [回测买入])
            position_value: 本笔仓位金额
            其他参数同上
        """
        # 记录买入
        self.record_buy(
            trade_date=buy_date,
            code=code,
            name=name,
            price=buy_price,
            shares=shares,
            sector=sector,
            style_group=style_group,
            alpha_score=alpha_score,
            alpha_trend=alpha_trend,
            mom_5=mom_5,
            mom_20=mom_20,
            sharpe=sharpe,
            rsi=rsi,
            bias_20=bias_20,
            atr=atr,
            action_label=action_label,
            position_value=position_value
        )
        
        # 记录卖出 (传入实际交易日天数)
        self.record_sell(
            trade_date=sell_date,
            code=code,
            name=name,
            price=sell_price,
            shares=shares,
            exit_reason=exit_reason,
            sector=sector,
            style_group=style_group,
            override_hold_days=actual_hold_days
        )

    def record_equity(
        self,
        trade_date: str,
        nav: float,
        daily_return: float = 0.0,
        cumulative_return: float = 0.0,
        drawdown: float = 0.0,
        position_count: int = 0,
        benchmark_nav: float = 1.0
    ):
        """
        记录每日净值
        
        Args:
            trade_date: 日期
            nav: 净值
            daily_return: 当日收益率
            cumulative_return: 累计收益率
            drawdown: 回撤
            position_count: 持仓数量
            benchmark_nav: 基准净值
        """
        equity_record = {
            'session_id': self.session_id,
            'trade_date': trade_date,
            'nav': nav,
            'daily_return': daily_return,
            'cumulative_return': cumulative_return,
            'drawdown': drawdown,
            'position_count': position_count,
            'benchmark_nav': benchmark_nav
        }
        self._equity_buffer.append(equity_record)

    def record_position_snapshot(
        self,
        trade_date: str,
        code: str,
        name: str,
        shares: int,
        cost_price: float,
        current_price: float,
        hold_days: int = 0
    ):
        """
        记录持仓快照
        
        Args:
            trade_date: 日期
            code: 股票代码
            name: 股票名称
            shares: 持仓数量
            cost_price: 成本价
            current_price: 当前价
            hold_days: 持仓天数
        """
        market_value = current_price * shares
        unrealized_pnl = (current_price - cost_price) * shares
        unrealized_pnl_pct = ((current_price / cost_price) - 1) * 100 if cost_price > 0 else 0
        
        position_record = {
            'session_id': self.session_id,
            'trade_date': trade_date,
            'code': code,
            'name': name,
            'shares': shares,
            'cost_price': cost_price,
            'current_price': current_price,
            'market_value': market_value,
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_pct': unrealized_pnl_pct,
            'hold_days': hold_days
        }
        self._positions_buffer.append(position_record)

    def flush(self):
        """将缓存数据批量写入数据库"""
        try:
            with get_db_connection() as conn:
                # 写入交易记录
                if self._trades_buffer:
                    for trade in self._trades_buffer:
                        conn.execute('''
                            INSERT INTO backtest_trades 
                            (session_id, trade_id, code, name, sector, style_group,
                             trade_type, trade_date, price, shares, amount, commission,
                             alpha_score, mom_5, mom_20, sharpe, rsi, bias_20, atr,
                             exit_reason, pnl, pnl_pct, hold_days, 
                             action_label, position_value, account_balance, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            trade['session_id'], trade['trade_id'], trade['code'],
                            trade['name'], trade['sector'], trade['style_group'],
                            trade['trade_type'], trade['trade_date'], trade['price'],
                            trade['shares'], trade['amount'], trade['commission'],
                            trade['alpha_score'], trade['mom_5'], trade['mom_20'],
                            trade['sharpe'], trade['rsi'], trade['bias_20'], trade['atr'],
                            trade['exit_reason'], trade['pnl'], trade['pnl_pct'],
                            trade['hold_days'], trade.get('action_label'), 
                            trade.get('position_value'), trade.get('account_balance'),
                            trade['created_at']
                        ))
                    logger.debug(f"[TradeRecorder] 写入 {len(self._trades_buffer)} 条交易记录")
                    self._trades_buffer.clear()
                
                # 写入净值曲线
                if self._equity_buffer:
                    for eq in self._equity_buffer:
                        conn.execute('''
                            INSERT INTO backtest_equity_curve
                            (session_id, trade_date, nav, daily_return, cumulative_return,
                             drawdown, position_count, benchmark_nav)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            eq['session_id'], eq['trade_date'], eq['nav'],
                            eq['daily_return'], eq['cumulative_return'],
                            eq['drawdown'], eq['position_count'], eq['benchmark_nav']
                        ))
                    self._equity_buffer.clear()
                
                # 写入持仓快照
                if self._positions_buffer:
                    for pos in self._positions_buffer:
                        conn.execute('''
                            INSERT INTO backtest_positions
                            (session_id, trade_date, code, name, shares, cost_price,
                             current_price, market_value, unrealized_pnl, unrealized_pnl_pct, hold_days)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            pos['session_id'], pos['trade_date'], pos['code'],
                            pos['name'], pos['shares'], pos['cost_price'],
                            pos['current_price'], pos['market_value'],
                            pos['unrealized_pnl'], pos['unrealized_pnl_pct'], pos['hold_days']
                        ))
                    self._positions_buffer.clear()
                    
        except Exception as e:
            logger.error(f"[TradeRecorder] 批量写入失败: {e}")

    def finalize(
        self,
        final_nav: float,
        total_return: float,
        annual_return: float,
        sharpe_ratio: float,
        max_drawdown: float,
        win_rate: float,
        total_trades: int
    ):
        """
        完成回测，更新会话统计信息
        
        Args:
            final_nav: 最终净值
            total_return: 总收益率 (%)
            annual_return: 年化收益率 (%)
            sharpe_ratio: 夏普比率
            max_drawdown: 最大回撤 (%)
            win_rate: 胜率 (%)
            total_trades: 总交易次数
        """
        # 先刷新缓存
        self.flush()
        
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            with get_db_connection() as conn:
                conn.execute('''
                    UPDATE backtest_sessions
                    SET end_time = ?,
                        final_nav = ?,
                        total_return = ?,
                        annual_return = ?,
                        sharpe_ratio = ?,
                        max_drawdown = ?,
                        win_rate = ?,
                        total_trades = ?,
                        status = 'completed'
                    WHERE session_id = ?
                ''', (
                    end_time, final_nav, total_return, annual_return,
                    sharpe_ratio, max_drawdown, win_rate, total_trades,
                    self.session_id
                ))
            logger.info(f"[TradeRecorder] 回测完成: {self.session_id} | 收益: {total_return:.2f}%")
        except Exception as e:
            logger.error(f"[TradeRecorder] 更新会话状态失败: {e}")

    def get_session_id(self) -> str:
        """获取当前会话 ID"""
        return self.session_id


# ============ 查询函数 (用于可视化) ============

def get_backtest_sessions(limit: int = 20) -> pd.DataFrame:
    """
    获取回测会话列表
    
    Args:
        limit: 返回数量限制
        
    Returns:
        会话列表 DataFrame
    """
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(f'''
                SELECT * FROM backtest_sessions
                ORDER BY start_time DESC
                LIMIT {limit}
            ''', conn)
        return df
    except Exception as e:
        logger.error(f"[DB] 查询会话失败: {e}")
        return pd.DataFrame()


def get_session_trades(session_id: str) -> pd.DataFrame:
    """
    获取指定会话的所有交易记录
    
    Args:
        session_id: 会话 ID
        
    Returns:
        交易记录 DataFrame
    """
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query('''
                SELECT * FROM backtest_trades
                WHERE session_id = ?
                ORDER BY trade_date, id
            ''', conn, params=(session_id,))
        return df
    except Exception as e:
        logger.error(f"[DB] 查询交易记录失败: {e}")
        return pd.DataFrame()


def get_session_equity_curve(session_id: str) -> pd.DataFrame:
    """
    获取指定会话的净值曲线
    
    Args:
        session_id: 会话 ID
        
    Returns:
        净值曲线 DataFrame
    """
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query('''
                SELECT * FROM backtest_equity_curve
                WHERE session_id = ?
                ORDER BY trade_date
            ''', conn, params=(session_id,))
        return df
    except Exception as e:
        logger.error(f"[DB] 查询净值曲线失败: {e}")
        return pd.DataFrame()


def get_session_positions(session_id: str, trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    获取指定会话的持仓快照
    
    Args:
        session_id: 会话 ID
        trade_date: 可选，指定日期
        
    Returns:
        持仓快照 DataFrame
    """
    try:
        with get_db_connection() as conn:
            if trade_date:
                df = pd.read_sql_query('''
                    SELECT * FROM backtest_positions
                    WHERE session_id = ? AND trade_date = ?
                    ORDER BY market_value DESC
                ''', conn, params=(session_id, trade_date))
            else:
                df = pd.read_sql_query('''
                    SELECT * FROM backtest_positions
                    WHERE session_id = ?
                    ORDER BY trade_date, market_value DESC
                ''', conn, params=(session_id,))
        return df
    except Exception as e:
        logger.error(f"[DB] 查询持仓快照失败: {e}")
        return pd.DataFrame()


def get_trade_statistics(session_id: str) -> Dict:
    """
    获取交易统计信息
    
    Args:
        session_id: 会话 ID
        
    Returns:
        统计信息字典
    """
    try:
        with get_db_connection() as conn:
            # 获取卖出交易 (只有卖出才有盈亏)
            df_sells = pd.read_sql_query('''
                SELECT * FROM backtest_trades
                WHERE session_id = ? AND trade_type = 'SELL'
            ''', conn, params=(session_id,))
            
            if df_sells.empty:
                return {}
            
            # 计算统计
            total_trades = len(df_sells)
            win_trades = len(df_sells[df_sells['pnl'] > 0])
            loss_trades = len(df_sells[df_sells['pnl'] < 0])
            
            avg_pnl = df_sells['pnl'].mean()
            avg_pnl_pct = df_sells['pnl_pct'].mean()
            max_win = df_sells['pnl_pct'].max()
            max_loss = df_sells['pnl_pct'].min()
            avg_hold_days = df_sells['hold_days'].mean()
            
            # 按板块统计
            sector_stats = df_sells.groupby('sector').agg({
                'pnl_pct': 'mean',
                'code': 'count'
            }).rename(columns={'code': 'trade_count', 'pnl_pct': 'avg_return'})
            
            # 按退出原因统计
            exit_stats = df_sells['exit_reason'].value_counts().to_dict()
            
            return {
                'total_trades': total_trades,
                'win_trades': win_trades,
                'loss_trades': loss_trades,
                'win_rate': win_trades / total_trades * 100 if total_trades > 0 else 0,
                'avg_pnl': avg_pnl,
                'avg_pnl_pct': avg_pnl_pct,
                'max_win_pct': max_win,
                'max_loss_pct': max_loss,
                'avg_hold_days': avg_hold_days,
                'sector_stats': sector_stats.to_dict() if not sector_stats.empty else {},
                'exit_stats': exit_stats
            }
    except Exception as e:
        logger.error(f"[DB] 计算交易统计失败: {e}")
        return {}


def delete_session(session_id: str) -> bool:
    """
    删除指定会话及其所有相关数据
    
    Args:
        session_id: 会话 ID
        
    Returns:
        是否成功
    """
    try:
        with get_db_connection() as conn:
            conn.execute('DELETE FROM backtest_trades WHERE session_id = ?', (session_id,))
            conn.execute('DELETE FROM backtest_equity_curve WHERE session_id = ?', (session_id,))
            conn.execute('DELETE FROM backtest_positions WHERE session_id = ?', (session_id,))
            conn.execute('DELETE FROM backtest_sessions WHERE session_id = ?', (session_id,))
        logger.info(f"[DB] 已删除会话: {session_id}")
        return True
    except Exception as e:
        logger.error(f"[DB] 删除会话失败: {e}")
        return False


def clear_all_backtest_data() -> bool:
    """
    清空所有回测数据 (用于每次回测前重置)
    
    Returns:
        是否成功
    """
    try:
        with get_db_connection() as conn:
            conn.execute('DELETE FROM backtest_trades')
            conn.execute('DELETE FROM backtest_equity_curve')
            conn.execute('DELETE FROM backtest_positions')
            conn.execute('DELETE FROM backtest_sessions')
        logger.info("[DB] 已清空所有回测历史数据")
        return True
    except Exception as e:
        logger.error(f"[DB] 清空回测数据失败: {e}")
        return False
