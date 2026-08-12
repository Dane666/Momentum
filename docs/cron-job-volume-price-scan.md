# cron-job.org 配置模板 — 价量盘后计划池 (`volume-price-scan.yml`)

由于本仓库 GitHub 原生 `schedule` 触发器不可靠（cron 事件曾停派发），
`volume-price-scan.yml` 改用 **cron-job.org → GitHub `workflow_dispatch` API** 作为主触发，
GitHub 自带 schedule 仅作兜底。

> 每次运行：下载基准库 → 导入板块缓存 → 增量预热到当日 → 扫描 → Bark 推送
> （有信号推计划池，无信号推回执）。

---

## 在 cron-job.org 新建作业

1. 打开 https://cron-job.org → 注册/登录 → **Cronjobs → Create cronjob**
2. **Title**：`momentum-volume-price-scan`
3. **URL**（POST 目标）：

```
https://api.github.com/repos/Dane666/Momentum/actions/workflows/volume-price-scan.yml/dispatches
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

- **非交易日自动跳过**：workflow 内含 `chinese_calendar.is_workday` 判定，周末/节假日
  扫描步骤直接跳过（不推送）。所以 cron 在周六日点火也不会产生误报，无需额外排程。
- **与 GitHub schedule 并存无妨**：若 GitHub 原生 schedule 偶尔也点火，两次运行
  结果一致（幂等），不会重复登记计划池（`add_picks` 按 date+code+type 去重）。
- **手动触发**：随时可执行
  `gh workflow run volume-price-scan.yml` 或 GitHub 网页 `Run workflow`。

---

## 同仓库其他 cron-job.org 作业（参考既有配置）

| 作业 | URL 路径 | Body | 计划(Asia/Shanghai) |
|------|----------|------|---------------------|
| 竞价扫描 | `.../auction-scan.yml/dispatches` | `{"ref":"main"}` | `25 1 * * 1-5` (09:25) |
| 尾盘选股(含低位绩优) | `.../momentum-scan.yml/dispatches` | `{"ref":"main","inputs":{"run_mode":"full"}}` | `25 14 * * 1-5` (14:25) |

> 账号时区若用 UTC，则上表 Hour 各减 8（09:25→01:25, 14:25→06:25）。

### 竞价扫描（`auction-scan.yml`，09:25）已并入「LightGBM 候选开盘核验」

自 `daily_inference` 推送候选清单持久化到 `data/push_candidates.json` 后，竞价扫描在 09:25 会**额外读取该候选 + 当日开盘**，生成「🤖 LightGBM 候选开盘核验（盘前弱市撤单预警）」段落，**并入竞价扫描报告、同一条 Bark 推送**（不再单独发推送）。

- 候选缺失 / 今日开盘未入库时该段自动跳过，不影响竞价全景主报告。
- 该作业已增加「恢复行情基线库 `db-baseline-v1`」与「预热候选 K 线（腾讯接口取今日开盘）」两步。
- ⚠️ 时序边界：9:20 后集合竞价单锁定，该核验为开盘后确认，不驱动撤单。
