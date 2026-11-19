# runner miss 日志（漏检回流与规则清理）

# 用途：记录"验证体系漏掉的 bug"如何转成判卷规则，以及判卷规则的季度清理。
# 两条转化路径（harness-workflow §4.7）：
#   路径一（miss → 规则）：同一类 miss 在 ≥2 个 PR 出现，且 3 个独立 finder 中 ≥2 个漏检 →
#     由 code owner 转成 REVIEW.md 的可判定条目（带触发器 + 事故依据），人审批准后入库。
#   路径二（bug finder 回放）：已知事故 PR 用 3 个 finder 回放，≥2 命中算可检测；
#     未达标的优先拆小 diff 或补规则。
# status=pending 表示记录尚需在组织侧执行；本文件固化格式与记录位。

schema_version: 1

miss_to_rule:
  threshold_pr: 2
  threshold_finder_miss: 2
  finder_count: 3
  approval: "code owner 写入 REVIEW.md 并经人审"
  entries: []

bug_finder_replay:
  hit_threshold: 2
  finder_count: 3
  entries: []

prune:
  cadence: "季度"
  triggers:
    - "触发器连续两季度未命中"
    - "检查已被常驻静态门覆盖"
    - "依据事故已修复且固定集回放连续为零"
  entries: []
