# runner golden set（元验证 / §4.8）

# 用途：验证"验证体系本身"能否发现已知 bug。样本用真实事故/缺口复现当时的仓库状态，
# 每例带一句话答案；只有正确描述该 bug 的 finding 才计分（见评分口径）。
# 本文件是固定评测集骨架：样本条目已按报告实证填入，回放执行与持续度量须在组织侧落地。

schema_version: 1

scoring:
  hit_rule: "finding 正确描述 answer_key 所指 bug 才计 1 分；其他 finding 不计分"
  severity_labels: [Important, Nit, Pre-existing]
  recall_stages: [raw, after_verify, final]

sets:
  fixed: "本文件 golden_cases（不随调参改动）"
  tuning: "governance/golden-set.tuning.md（另行维护，不参与正式评分）"
  stop_rule: "调到再调即过拟合固定集时停止"

continuous_metrics:
  - "merged 后被 revert 的数量"
  - "maintainer 合入前拒绝的数量"
  - "体量相当且一直干净的变更对照组"

# 固定集样本：来自 2026-11-18 runner 审计报告实证缺口。
golden_cases:
  - id: GC-01
    source: "S1/S5 / CI 注释漂移"
    answer_key: "ci.yml 注释称无 *_test.go，实为 14 个测试文件且全绿；注释失实会让人误以为测试未接"
    severity: Important
    status: pending
  - id: GC-02
    source: "S5 / 文档一致性盲区"
    answer_key: "ci.yml 引用不存在的 docs/ci-build-pipeline.md，doc_consistency 只扫 markdown 链接故漏报"
    severity: Nit
    status: pending
  - id: GC-03
    source: "S / D7 证据协议"
    answer_key: "无 verify manifest，done 靠自述而非真跑系统（WS 回连健康检查）的可观测状态与证据落点"
    severity: Important
    status: pending
  - id: GC-04
    source: "报告 / 回流未闭环"
    answer_key: "canary CurrentWeight / agent_version / TaskRun 产物不回传 hub，测试若依赖回流会假绿"
    severity: Important
    status: pending

bug_finder_replay:
  rule: "3 个独立 bug finder 对同一事故 diff 回放；≥2 命中算可检测；小 diff 漏检优先转 REVIEW 规则"
  status: pending
