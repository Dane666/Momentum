# cron-job.org 配置模板 — LightGBM 每日推理 (`daily-inference.yml`)

由于本仓库 GitHub 原生 `schedule` 触发器不可靠（cron 事件曾停派发，2026-08-04 确诊），
`daily-inference.yml` 改用 **cron-job.org → GitHub `workflow_dispatch` API** 作为主触发，
GitHub 自带 schedule 仅作兜底。

> 每次运行：下载基准库 → 导入板块缓存 → 增量预热到当日 → 生成因子面板 →
> 从 release `lgbm-model-v1` 下载模型 → 打分 → Top10 → Bark 推送（只读推荐，不自动交易）。

---

## 在 cron-job.org 新建作业

1. 打开 https://cron-job.org → 注册/登录 → **Cronjobs → Create cronjob**
2. **Title**：`momentum-daily-inference`
3. **URL**（POST 目标）：

```
https://api.github.com/repos/Dane666/Momentum/actions/workflows/daily-inference.yml/dispatches
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
  `gh workflow run daily-inference.yml` 或 GitHub 网页 `Run workflow`。

---

## 同仓库其他 cron-job.org 作业（参考既有配置）

| 作业 | URL 路径 | Body | 计划(Asia/Shanghai) |
|------|----------|------|---------------------|
| 竞价扫描 | `.../auction-scan.yml/dispatches` | `{"ref":"main"}` | `25 1 * * 1-5` (09:25) |
| 尾盘选股(含低位绩优) | `.../momentum-scan.yml/dispatches` | `{"ref":"main","inputs":{"run_mode":"full"}}` | `25 14 * * 1-5` (14:25) |
| 价量盘后计划池 | `.../volume-price-scan.yml/dispatches` | `{"ref":"main"}` | `30 15 * * 1-5` (15:30) |
| **LightGBM 推理** | `.../daily-inference.yml/dispatches` | `{"ref":"main"}` | `30 15 * * 1-5` (15:30) |

> 账号时区若用 UTC，则上表 Hour 各减 8。
