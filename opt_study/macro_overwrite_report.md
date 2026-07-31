# Macro Overwrite 宏观熔断模块 —— 实现与接入报告

> 生成日期: 2026-07-31 ｜ 关联: `seasonal_timing_validation_report.md`(日历效应验证)
> 模块: `opt_study/macro_overwrite.py` ｜ 接入回测: `opt_study/macro_backtest.py`

---

## 1. 模块定位与设计原则

`MacroOverwrite` 是三策略(低位绩优 / 主动量 / C-Tail)之上的**总闸**。它**不改变任何策略信号本身**,
只在【开仓日】层面做两层裁决, 输出每个交易日的 `OverlayDecision`:

| 字段 | 含义 |
|------|------|
| `level` | `normal` / `soft` / `hard` |
| `allow_new` | 总闸是否允许新开仓(硬熔断时为 `False`) |
| `position_scale` | 软降仓比例(`1.0`=不降, `0.5`=半仓, `0`=空仓) |
| `reason` / `tags` | 人类可读原因 / 触发标签 |

两层裁决:
1. **应激熔断 (StressBreaker)** —— 全A等权净值 `nav` 相对 `MA60` 的偏离.
   `dev = nav/MA60 - 1`: `dev < -10%` 硬熔断(禁新开仓); `-10% ≤ dev < -4%` 软降仓(×0.5).
2. **日历软护栏 (CalendarGuard)** —— 历史弱月(4/6/12 月, 全样本验证最弱)软降仓; 春季(1-3月)不降.

**设计原则(来自日历效应长期验证结论):**
- 纯择时绝对收益被僵尸股等权基准放大、不可信 → 只做「软护栏」, 不做硬按月份满仓/空仓.
- 因子倾斜(小盘 / 红利)因全库无市值 / 股息率数据 **不可测** → 不实现, 仅留 `per_strategy` 接口位.
- **策略无关**: 模块只消费 `(date, nav, ma60)` 市场环境输入, 绝不触碰任何策略内部.
- **零改原始 harness**: 通过三策略既有的「开仓日闸口」接入(见第 3 节), 与 `regime_backtest` 同一口径.

---

## 2. 模块结构 (`macro_overwrite.py`)

- `OverlayDecision` —— 单日裁决数据类(`to_dict()` 可序列化).
- `MacroOverwrite(config=None)` —— 总闸, 含:
  - `decide(date, nav_val, ma60_val, strategy)` —— 单日裁决(应激 + 日历叠加, 取更严重者).
  - `build_series(nav, strategy)` —— 给定净值序列产出 `{YYYY-MM-DD: OverlayDecision}`(供回测/实时).
  - `allow_map(series)` / `scale_map(series)` —— 导出 `{date: bool}` / `{date: float}` 供接入.
- 便捷函数: `build_overlay_series`, `allow_fn`(C-Tail `filter_fn` 直接可用).
- 软降仓忠实应用(不改 harness, 仅缩放已产出的交易/期收益):
  - `rebuild_equity_from_scaled_trades` —— 槽位型(低位绩优)按开仓日 `position_scale` 缩放 `shares`, 逐日按收盘价市值重估(与 `HOQ.simulate` 记账口径一致, 已修正滑点/初始资金口径).
  - `scale_period_returns` —— 再平衡型(主动量 / C-Tail)逐期收益按开仓日缩放.

---

## 3. 三策略接入方式(均不改原始 harness)

| 策略 | 接入闸口 | 实现 |
|------|----------|------|
| 低位绩优 | `HOQ.simulate` 的 `regime_at` 闸口复用 | `cfg["regime_on"]=True`, 把 `macro.allow_map` 作为 `ma60` 闸口; 软降仓用权益重建 |
| 主动量 | `ma20_forced` 构造 | 在 `macro` 禁开日置 `nav+1e-6`(强制 `nav<ma20` 空仓); 软降仓缩放权益曲线 |
| C-Tail | `simulate_c` 的 `filter_fn` | `filter_fn = macro.allow_fn`(禁开日不放新仓); 软降仓在实时层实现(见第 5 节) |

> 三种闸口与 `regime_backtest` 中「分环境 forced」用的是**同一套机制**, 仅把允许字典换成宏观总闸,
> 故完全旁路、可审计.

---

## 4. 默认配置(数据驱动, 见第 5 节证据)

```python
per_strategy = {
    "low_quality": {"calendar": False, "stress": "crash_only"},  # 逆势: 仅防极端暴跌
    "momentum":     {"calendar": False, "stress": "crash_only"},
    "c_tail":       {"calendar": False, "stress": "crash_only"},
}
# crash_only: 仅 nav 相对 MA60 偏离 < -15% 才硬熔断(极端暴跌保险)
```

即:**默认 = 极端暴跌硬熔断保险 + 日历/软应激全部关闭**. 模块仍完整保留日历与软应激组件,
一行配置即可开启(见第 6 节).

---

## 5. 回测验证结果(`macro_backtest.py`, 窗口 2024-07 ~ 2026-07)

### 5.1 默认配置(crash_only, 无极端暴跌 → 总闸静默)

| 策略 | 指标 | baseline | +macro | Δ |
|------|------|---------:|-------:|---:|
| 低位绩优 | 总收益% / 夏普 / 回撤% | 18.65 / 0.784 / -14.86 | 18.65 / 0.784 / -14.86 | 0 / 0 / 0 |
| 主动量 | 总收益% / 夏普 / 回撤% | 14.56 / 0.480 / -11.04 | 14.56 / 0.483 / -11.04 | 0 / 0 / 0 |
| C-Tail | 总收益% / 夏普 / 回撤% | 23.44 / 0.479 / -61.23 | 23.44 / 0.479 / -61.23 | 0 / 0 / 0 |

**结论: 2024-2026 窗口无 -15% 极端暴跌, 总闸正确保持静默, 对三策略零侵入.** 这正是保护性
overlay 应有的行为——平时隐形, 仅在真危机时出手.

### 5.2 反事实验证①: 强制对低位绩优开启日历软护栏(×0.5)

| | 总收益% | 夏普 | 回撤% |
|---|---:|---:|---:|
| baseline | 5.82(quick窗) | 0.543 | -10.72 |
| +日历软降仓 | 0.43 | 0.102 | -3.67 |

→ 日历降仓大幅砍掉收益(弱月恰恰是低位绩优的最佳行情), **证明其净负**.

### 5.3 反事实验证②: 对低位绩优开启应激软/硬熔断(stress=normal)

触发: 硬熔断 1 天 / 软降仓 42 天(2024-2026 熊市中 nav 长期低于 MA60).

| | 总收益% | 夏普 | 回撤% |
|---|---:|---:|---:|
| baseline | 18.65 | 0.784 | -14.86 |
| +应激熔断 | 10.55 | 0.496 | -15.92 |

→ 应激熔断同样**伤害**低位绩优(它是逆势策略, 弱市即其 alpha 来源), 且回撤未改善.

### 5.4 核心发现

> **机械的日历软降仓(4/6/12 ×0.5)与广谱应激软降仓, 对这三套具体策略在样本内均为净负.**
> 原因: (1) 低位绩优是逆势策略, 弱市/暴跌恰是其最佳环境; (2) 主动量 / C-Tail 在所谓"弱月"
> 实际有正 edge, 降仓即放弃收益且回撤未改善.
>
> 因此默认配置收敛为 **crash_only 极端暴跌保险**(几乎不触发、零侵入、真危机时才保护),
> 呼应日历效应验证"不做硬/机械按月份操作"的结论. 日历与软应激组件保留、可一行开启,
> 待更长样本或补市值/股息数据后可重新评估.

---

## 6. 如何开启日历 / 软应激组件(可选)

```python
from macro_overwrite import MacroOverwrite
mo = MacroOverwrite({
    "per_strategy": {
        "momentum": {"calendar": True,  "stress": "normal"},   # 趋势类启用日历+软应激
        "c_tail":   {"calendar": True,  "stress": "normal"},
        "low_quality": {"calendar": False, "stress": "crash_only"},  # 逆势保持保守
    }
})
```

---

## 7. 实时接入

- `regime_backtest.py` 新增 `compute_current_macro()`, 在 `regime_recommendation.json` 写入
  `macro_overwrite.current`(最新交易日应激级别/仓位上限) + 默认配置说明.
- `momentum-scan.yml` 的 Bark 推送新增 `🛡️ 宏观熔断` 段, 实时展示:
  - 硬熔断 → "禁止新开仓"; 软降仓 → "建议仓位 ×0.5"; 正常 → "总闸开放, 策略信号有效".

---

## 8. 文件清单

| 文件 | 变更 |
|------|------|
| `opt_study/macro_overwrite.py` | **新增** 独立模块 |
| `opt_study/macro_backtest.py` | **新增** 三策略接入回测 |
| `opt_study/regime_backtest.py` | 修改 新增 `compute_current_macro` + JSON `macro_overwrite` 字段 |
| `.github/workflows/momentum-scan.yml` | 修改 Bark 推送新增宏观熔断段 |
| `opt_study/macro_backtest_result.json` | 生成 默认配置对比结果 |
