# cron-job.org 配置模板 — LightGBM 每日推理 (`daily_inference.yml`)

> ## ℹ️ 当前状态：**用 GitHub 原生 cron 定时（19:07 CST）+ 手动兜底；本文档为备用方案**
>
> `daily_inference.yml` 现已启用 GitHub 原生 `schedule`：`cron: '7 11 * * 1-5'`（11:07 UTC = 19:07 CST，周一至周五）。
>
> **为什么这个工作流可以用不可靠的原生 cron**：它产出的是**次日开盘才用**的推荐，对时延不敏感。
> 即使当晚漏跑，次日开盘前手动 Run workflow 补跑即可（数据同源，结果一致）——
> 这与盘中价量扫描（错过 15:30 就没意义）有本质区别。
>
> 时点选择 19:07 而非整点/半点：刻意避开本仓库其他工作流集中的 07:30~08:30 UTC，
> 也避开 GitHub cron 排队最严重的整点/半点，以提高派发成功率。
>
> **本文档保留为备用**：若日后观察到原生 cron 漏跑过于频繁（如连续一周不派发），
> 再按下文升级为 cron-job.org 精确触发。升级步骤：① 按下文建 cron-job.org 作业 →
> ② 可保留 `daily_inference.yml` 的 `schedule` 作为双保险（重复触发无害，仅多跑一次）。

由于本仓库 GitHub 原生 `schedule` 触发器不可靠（cron 事件曾停派发，2026-08-04 确诊
`volume-price-scan` / `momentum-scan` 均漏派发），**对时延敏感的工作流**应改用
**cron-job.org → GitHub `workflow_dispatch` API** 作为主触发，GitHub 自带 schedule 仅作兜底。

> **注意文件名用下划线**：工作流文件是 `daily_inference.yml`（不是 `daily-inference.yml`）。
> API 路径必须与文件名完全一致，否则返回 404 "workflow not found"。

> 每次运行：下载基准库 → 导入板块缓存 → 增量预热到当日 → 生成因子面板 →
> 从 release `lgbm-model-v1` 下载模型 → 打分 → Top10 → Bark 推送（只读推荐，不自动交易）。

---

## 在 cron-job.org 新建作业

1. 打开 https://cron-job.org → 注册/登录 → **Cronjobs → Create cronjob**
2. **Title**：`momentum-daily-inference`
3. **URL**（POST 目标）：

```
https://api.github.com/repos/Dane666/Momentum/actions/workflows/daily_inference.yml/dispatches
```

4. **Request method**：`POST`
5. **Request headers**（逐行添加）：

```
Accept: application/vnd.github+json
Authorization: Bearer <你的_GITHUB_PAT>
Content-Type: application/json
X-GitHub-Api-Version: 2022-11-28
```

   - `<你的_GITHUB_PAT>` = 有 `repo` / `actions:write` 权限的 Personal Access Token
   - 创建地址：GitHub → Settings → Developer settings → Personal access tokens → Fine-grained
     （勾选该仓库的 `Actions: write`）

6. **Request body**（raw JSON）：

```json
{"ref":"main"}
```

7. **Schedule（计划时间）**：每个交易日 **15:30（北京时间）盘后**触发
   - 若 cron-job.org 账号时区设为 **Asia/Shanghai**：
     ```
     Minute: 30   Hour: 15   Day: *   Month: *   Weekday: 1-5
     ```
   - 若账号时区设为 **UTC**（推荐，避免误解）：
     ```
     Minute: 30   Hour: 7    Day: *   Month: *   Weekday: 1-5
     ```
     （15:30 CST = 07:30 UTC）

8. **Save** 即可。

---

## 说明

- **非交易日自动跳过**：workflow 内含 `chinese_calendar.is_workday` 判定（在因子预热/打分步骤前），
  周末/节假日扫描步骤直接跳过（不推送）。cron 在周六日点火也不会产生误报。
- **与 GitHub schedule 并存无妨**：若 GitHub 原生 schedule 偶尔也点火，两次运行结果一致（幂等）。
- **模型更新**：重新训练后，本地跑 `tasks/model_training/05_export_model.py` 生成新 pkl，
  再 `gh release upload lgbm-model-v1 --clobber tasks/model_training/models/model_v1.pkl tasks/model_training/models/model_meta.json`
  覆盖 release 资产即可，无需改 workflow。
- **手动触发**：随时可执行
  `gh workflow run daily_inference.yml` 或 GitHub 网页 `Run workflow`。

---

## 同仓库其他 cron-job.org 作业（参考既有配置）

| 作业 | URL 路径 | Body | 计划(Asia/Shanghai) |
|------|----------|------|---------------------|
| 竞价扫描 | `.../auction-scan.yml/dispatches` | `{"ref":"main"}` | `25 1 * * 1-5` (09:25) |
| 尾盘选股(含低位绩优) | `.../momentum-scan.yml/dispatches` | `{"ref":"main","inputs":{"run_mode":"full"}}` | `25 14 * * 1-5` (14:25) |
| 价量盘后计划池 | `.../volume-price-scan.yml/dispatches` | `{"ref":"main"}` | `30 15 * * 1-5` (15:30) |
| **LightGBM 推理** | `.../daily_inference.yml/dispatches` | `{"ref":"main"}` | ℹ️ 当前**不用 cron-job.org**，改由 GitHub 原生 cron `7 11 * * 1-5` UTC（19:07 CST）触发 |

> 账号时区若用 UTC，则上表 Hour 各减 8。

---

## 开盘弱市预警（已并入竞价扫描，无需单独 cronjob）

「LightGBM 候选开盘核验（盘前弱市撤单预警）」**不再**作为 `daily_inference.yml` 的独立 `open_warn` 作业 + `repository_dispatch` 触发。
自本改动起，它并入 `auction-scan.yml`（竞价扫描，09:25）：

- 候选清单由夜盘 `daily_inference` 推送后持久化到 `data/push_candidates.json`（已提交）；
- 次日 09:25 `auction-scan.yml` 复用现有 cron-job.org 配置，读取候选 + 今日开盘，作为「🤖 LightGBM 候选开盘核验」段落并入竞价扫描报告、同一条 Bark 推送。

> 因此：**不要**再为 `daily_inference.yml` 配置 `repository_dispatch(daily-inference)` 类型的 cron-job.org；开盘预警已随竞价扫描（上表 09:25 那条）一并触发。
> 时序边界：9:20 后集合竞价单锁定，该核验为开盘后确认，不驱动撤单；如需驱动撤单须将竞价扫描提前至 ≤9:19 用指示性撮合价触发（另行实现）。
