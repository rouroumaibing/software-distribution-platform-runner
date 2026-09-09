# Makefile — software-distribution-platform-runner 本地管理
# 原生工具链（Go 项目）：make <target>
# 部署形态（start-deploy / stop-deploy）复用工作区根 deploy-local.sh / undeploy-local.sh
#
# clean       = 仅删【本仓生成物】，保留下载的依赖/工具链
# clean-deep  = 本仓彻底清理（生成物 + 仓内下载依赖）
# 注意：绝不触碰工作区根共享资源（../.bin 工具链 / ../.kubeconfig / ../.dockerconfig）——
#       它们属部署形态、由 deploy-local.sh 管理（.kubeconfig/.dockerconfig 指向的文件/目录
#       还可能被别的仓/脚本共用），仓级清理越界会破坏联调环境。
# 一律用显式路径删除，绝不触碰源码（cmd/ internal/ pkg/ go.mod ...）
# 注意：controller-gen 生成文件(api/v1alpha1/zz_generated.deepcopy.go, config/crd/bases,
#       config/rbac) 属于打包输入，clean / clean-deep 均不删（显式路径删除天然避开）。

APP := runner
WS_ROOT := $(abspath $(CURDIR)/..)
BIN := bin/$(APP)

.PHONY: help clean clean-deep build package start-dev stop-dev start-deploy stop-deploy

help:
	@echo "targets:"
	@echo "  clean        仅删本仓生成物(bin/.run/output/coverage)，保留 controller-gen 生成文件"
	@echo "  clean-deep   本仓彻底清理（生成物 + 仓内下载依赖；不碰工作区共享工具链）"
	@echo "  build        编译二进制到 $(BIN)"
	@echo "  package      组件打包：构建镜像并推送本地 registry (build/$(APP)/build.sh)"
	@echo "  start-dev    启动本地开发服务(后台, pid 文件 + 进程组管理)"
	@echo "  stop-dev     停止本地开发服务(按 pid 文件 + 进程清理)"
	@echo "  start-deploy 调用 $(WS_ROOT)/deploy-local.sh 全量部署(kind+helm)"
	@echo "  stop-deploy  调用 $(WS_ROOT)/undeploy-local.sh 卸载"

# 生成物目录（clean 与 clean-deep 都会删；绝不删源码 / 下载依赖 / controller-gen 生成文件）
GEN_DIRS := bin .run output coverage

clean:
	@echo "[clean:$(APP)] 删除生成物 ($(GEN_DIRS))，保留依赖/工具链 与 controller-gen 生成文件"
	rm -rf $(addprefix $(CURDIR)/,$(GEN_DIRS))
	# 散落单文件生成物
	find $(CURDIR) -maxdepth 2 \( -name '*.test' -o -name '*.out' -o -name '*.err.txt' -o -name '.DS_Store' -o -name '*.swp' \) -delete 2>/dev/null
	@echo "[clean:$(APP)] done."

clean-deep: clean
	@echo "[clean:$(APP)] done (deep)."

build:
	go build -o $(BIN) ./cmd/$(APP)

package:
	bash build/$(APP)/build.sh

start-dev:
	@bash hack/svc.sh start

stop-dev:
	@bash hack/svc.sh stop

start-deploy:
	@bash $(WS_ROOT)/deploy-local.sh

stop-deploy:
	@bash $(WS_ROOT)/undeploy-local.sh
