# software-distribution-platform-runner
软件发布平台任务代理。技术栈：go、kubernetes Operator（kubebuilder / controller-runtime）。

## 本地开发 / 管理命令

管理命令入口为根 `Makefile`（`make <target>`，`make help` 可列出全部）；服务启停、清理的底层实现见 `hack/svc.sh`。打包/部署形态说明：

- `package` 产物（交付包）默认版本 `v0.0.1`，与工作区根 `deploy-local.sh` 的默认部署版本一致；
- `start-deploy` / `stop-deploy` 复用工作区根 `deploy-local.sh` / `undeploy-local.sh`；
- controller-gen 生成文件（`api/v1alpha1/zz_generated.deepcopy.go`、`config/crd/bases/*.yaml`、`config/rbac/*.yaml`）属构建/打包输入，**clean 不删、保留**。

| make target | 作用 |
| --- | --- |
| `make build` | 编译二进制到 `bin/runner`（`go build ./cmd/runner`） |
| `make package` | 组件打包：交叉编译 → 运行时镜像 + docker save + charts → 交付包 `output/software-distribution-platform-runner-<version>.tar.gz`，并尝试推送本地 registry（`build/runner/build.sh`） |
| `make start-dev` | 启动本地开发服务（`hack/svc.sh start`：pid 文件 + 进程组管理，启动前自动清理旧实例；`go run ./cmd/runner`） |
| `make stop-dev` | 停止本地开发服务（`hack/svc.sh stop`，按 pid 文件 + 进程特征兜底清理） |
| `make clean` | 仅删生成物（`bin/ .run/ output/ coverage/` 及散落单文件），保留下载依赖与工具链、保留 controller-gen 生成文件 |
| `make clean-deep` | 本仓彻底清理（删生成物，同 `clean`）；不删下载依赖/工具链，绝不触碰工作区共享资源（`../.bin` / `../.kubeconfig` / `../.dockerconfig`，属部署形态由 `deploy-local.sh` 管理）；同样保留 controller-gen 生成文件 |
| `make start-deploy` | 本地全量部署：调用工作区根 `deploy-local.sh`（kind + helm 交付形态） |
| `make stop-deploy` | 本地全量卸载：调用工作区根 `undeploy-local.sh`（保留 kind 集群） |

## 设计文档

本组件的设计文档（实现 Story、kubebuilder 安装、任务处理与 DAG 推进、授权边界等）已统一收敛到独立的 [`software-distribution-platform-docs`](https://github.com/rouroumaibing/software-distribution-platform-docs) 仓库（单一真源），本仓库不再存放设计文档正文。

- 实现 Story：[`runner/STORY-runner-implementation.md`](https://github.com/rouroumaibing/software-distribution-platform-docs/blob/main/runner/STORY-runner-implementation.md)
- 跨组件对齐（整体目标 / 授权模型 G7 / 执行模型）：见 docs 仓库 [`README.md` §5](https://github.com/rouroumaibing/software-distribution-platform-docs/blob/main/README.md)

> 本仓库 `docs/design/README.md` 仅保留一个指针，指向上述统一文档库；设计文档的修改请在 docs 仓库进行。
