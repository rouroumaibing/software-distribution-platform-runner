#cloud-config
# =============================================================================
# Runner 无人值守安装 cloud-init 模板（ADR-0002）
#
# 用法：装机管线先用 hub join-bundle API（POST /api/v1/targets/:id/join-bundle，
# 用户 RBAC）取得安装数据，替换占位符后作为 userdata 注入目标机：
#   __CA_PEM__         join-bundle 返回的 caPem（多行 PEM，**每行缩进 6 空格**对齐块标量）
#   __JOIN_ENV_FILE__  join-bundle 渲染的环境文件（console「复制 env」产物，**每行缩进 4 空格**）
#   __RUNNER_BINARY_URL__ runner 二进制下载地址（由装机管线决定，平台不托管；ADR-0002 §6）
#
# 信任锚说明：本模板走「CA 内嵌 userdata」路径（IaC 即带外通道）。云上若网关
# 入口已换公网证书（Web PKI），可去掉 ca.crt 落盘与指纹校验两段，runner 凭
# OS 信任根直连。
#
# 手工（人工在场）安装不必用本模板：console 目标页「安装命令」弹窗复制脚本即可。
# =============================================================================

write_files:
  - path: /etc/sdp-runner/ca.crt
    permissions: '0644'
    content: |
      __CA_PEM__

  - path: /etc/sdp-runner/sdp-runner.env
    permissions: '0600'
    content: |
      __JOIN_ENV_FILE__

  - path: /etc/systemd/system/sdp-runner.service
    permissions: '0644'
    content: |
      [Unit]
      Description=SDP Runner Agent
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      EnvironmentFile=/etc/sdp-runner/sdp-runner.env
      ExecStart=/usr/local/bin/sdp-runner
      Restart=always
      RestartSec=5
      # 前置失败（配置缺失/CA 校验不过）不无限重启，让装机管线能从日志定位。
      StartLimitBurst=5
      StartLimitIntervalSec=120

      [Install]
      WantedBy=multi-user.target

runcmd:
  # 指纹自校验（kubeadm 语义）：对拿到的 CA 验 SPKI SHA256，防 userdata 注入通道被篡改。
  # 指纹来自 join-bundle 的 SDP_HUB_CA_FINGERPRINT（console 渲染的 env 文件自带该行）。
  - |
    set -eu
    install -d -m 0750 /etc/sdp-runner
    EXPECTED_FP="$(awk -F'sha256:' '/SDP_HUB_CA_FINGERPRINT/{print $2}' /etc/sdp-runner/sdp-runner.env)"
    if [ -n "${EXPECTED_FP}" ]; then
      ACTUAL_FP="$(openssl x509 -in /etc/sdp-runner/ca.crt -noout -fingerprint -sha256 | sed 's/^.*=//' | tr -d ':' | tr 'A-F' 'a-f')"
      if [ "${ACTUAL_FP}" != "${EXPECTED_FP}" ]; then
        echo "sdp-runner: CA fingerprint mismatch (want ${EXPECTED_FP}, got ${ACTUAL_FP})" >&2
        exit 1
      fi
    fi
  # runner 二进制分发由装机管线决定（ADR-0002 §6 遗留：平台暂不托管安装包）。
  # 下载后落位 systemd 期望路径；私有网络下 --cacert 指向刚落盘的 CA。
  - |
    set -eu
    curl -fsSL --cacert /etc/sdp-runner/ca.crt -o /usr/local/bin/sdp-runner __RUNNER_BINARY_URL__
    chmod 0755 /usr/local/bin/sdp-runner
  - systemctl daemon-reload
  - systemctl enable --now sdp-runner
