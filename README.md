# software-distribution-platform-runner
软件发布平台任务代理。技术栈：go、kubernetes Operator（kubebuilder / controller-runtime）。

## 本地开发 / 管理命令

管理命令入口为本仓 `Makefile`（`make <target>`，`make help` 可列出全部）；服务启停、清理的底层实现见 `hack/svc.sh`。**本仓自包含：所有命令只依赖仓内脚本（`hack/svc.sh`、`build/runner/build.sh`），不依赖仓库外的任何脚本。** 打包/部署形态说明：

- **构建产物统一落在 `output/` 下**（`make build` 的 `output/bin/runner`、`make package` 的 `output/{charts,images}/` 与交付包），清理即一条 `rm -rf output`；
- `package` 产物（交付包）默认版本 `v0.0.1`；
- **起服务**：`make start-dev`（后台运行，pid 文件 + 进程组管理）→ `make stop-dev`；产出镜像 / 交付包用 `make package`；
- controller-gen 生成文件（`api/v1alpha1/zz_generated.deepcopy.go`、`config/crd/bases/*.yaml`、`config/rbac/*.yaml`）属构建/打包输入，**clean 不删、保留**；同一规则的还有 hub 的 swaggo docs（`docs/{docs.go,swagger.json,swagger.yaml}`：已入库、clean 默认保留，见 hub README「swaggo API 文档」）。

| make target | 作用 |
| --- | --- |
| `make build` | 编译二进制到 `output/bin/runner`（`go build ./cmd/runner`） |
| `make package` | 组件打包：交叉编译 → 运行时镜像 + docker save + charts → 交付包 `output/software-distribution-platform-runner-<version>.tar.gz`，并尝试推送本地 registry（`build/runner/build.sh`） |
| `make start-dev` | 启动本地开发服务（`hack/svc.sh start`：pid 文件 + 进程组管理，启动前自动清理旧实例；`go run ./cmd/runner`） |
| `make stop-dev` | 停止本地开发服务（`hack/svc.sh stop`，按 pid 文件 + 进程特征兜底清理） |
| `make clean` | **先停本地服务**，再删生成物（`output/ .run/ coverage/`、历史位置 `bin/`、仓根裸编译二进制、散落单文件），保留下载依赖与工具链、保留 controller-gen 生成文件 |
| `make clean NO_STOP=1` | 同上，但跳过停服务（CI / 无服务场景） |
| `make clean-deep` | 本仓彻底清理（删生成物，同 `clean`）；不删下载依赖/工具链，删除范围严格限定在本仓目录内（不触碰仓库外的共享资源）；同样保留 controller-gen 生成文件 |

## 行为要点（接入侧代理）

- **无入站 HTTP**：runner 出站回连 hub 的 WS 网关（`/gateway/ws`，`GATEWAY_TOKEN` 认证），断线按指数退避重连；**D-01**：重连后 informer cache sync 超时（约 2 分钟）会**主动退出**交由容器重启兜底——hub 多次重启窗口期可能出现短暂 CrashLoopBackOff，属设计行为，pod 重建即恢复。
- **自身不执行任务**：把 hub 下发的 spec 翻译成目标集群里的 K8s Job（`pkg/executor/job_builder.go`：EmptyDir 工作区 + main/release 容器跑脚本 / `helm upgrade --install` / `kubectl apply`）；只消费 hub 已鉴权下发的 spec，自身无授权逻辑（集群侧权限由 k8s RBAC 约束，见 docs 仓 runner Story §4.4）。
- **任务类型**：`Build` / `Test` / `Release` / `Approval`（`api/v1alpha1/pipelinerun_types.go` 枚举校验；controller 按类型分支 reconcile）。

## 设计文档

本组件的设计文档（实现 Story、kubebuilder 安装、任务处理与 DAG 推进、授权边界等）已统一收敛到独立的 [`software-distribution-platform-docs`](https://github.com/rouroumaibing/software-distribution-platform-docs) 仓库（单一真源），本仓库不再存放设计文档正文。

- 实现 Story：[`runner/STORY-runner-implementation.md`](https://github.com/rouroumaibing/software-distribution-platform-docs/blob/main/runner/STORY-runner-implementation.md)
- 跨组件对齐（整体目标 / 授权模型 G7 / 执行模型）：见 docs 仓库 [`README.md` §5](https://github.com/rouroumaibing/software-distribution-platform-docs/blob/main/README.md)

> 本仓库 `docs/design/README.md` 仅保留一个指针，指向上述统一文档库；设计文档的修改请在 docs 仓库进行。
