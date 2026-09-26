// Package executor turns a TaskRunSpec into a concrete Kubernetes Job —
// this is where the design discussed for script-based task execution
// (git checkout + artifact download as init containers, `sh {ScriptPath}
// {ScriptArgs...}` as the main container) actually gets built.
//
// 2026-09-26 E2E 补齐批次：
//   - G-5（runner 半边）：run params 注入为容器 env（裸 $KEY 引用）；
//     hub 半边在触发时做 ${KEY} 文本替换，两者互补。
//   - G-6：consume init 容器真实化 —— 调 hub `POST /artifacts/storage-url`
//     签发 GET URL 并下载到 workspace（此前只 echo 占位）。
//   - G-7：四个默认 job 镜像全部可经 JobBuilder 字段 / runner env 覆盖
//     （受限 registry 环境开箱即败 → 换成私有 mirror 镜像即可）。
//   - G-4：Privileged 任务容器以 privileged=true 运行（dind/cind 构建镜像）。
package executor

import (
	"regexp"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

const workspaceMountPath = "/workspace"

// standard images used for the plumbing init containers; overridable via
// JobBuilder fields (wired from SDP_JOB_IMAGE_* env in cmd/runner/main.go)
// so a self-hosted registry mirror can be swapped in without touching call
// sites (G-7).
const (
	// DefaultGitImage / DefaultArtifactImage / DefaultHelmImage /
	// DefaultKubectlImage are exported so cmd/runner can wire env overrides
	// without this package importing os (G-7: all four are overridable via
	// SDP_JOB_IMAGE_* runner env).
	DefaultGitImage      = "alpine/git:2.45.2"
	DefaultArtifactImage = "curlimages/curl:8.8.0" // G-6：consume 需要 curl + sh

	// DefaultReleaseImage* are placeholders; swap for the org's pinned
	// helm/kubectl images (or a self-built release tool) before production.
	DefaultHelmImage    = "alpine/helm:3.14.4"
	DefaultKubectlImage = "bitnami/kubectl:1.30" // 占位:换成实际 kubectl 镜像
)

// reservedEnv is the denylist for plain-name param → env injection (G-5):
// a user param named PATH/HOME/LD_PRELOAD would break or escalate inside the
// task container, so those names are skipped (use ${KEY} hub substitution or
// a prefixed name instead).
var reservedEnv = map[string]bool{
	"PATH": true, "HOME": true, "SHELL": true, "PWD": true, "IFS": true,
	"LD_PRELOAD": true, "LD_LIBRARY_PATH": true, "PYTHONPATH": true,
	"KUBERNETES_SERVICE_HOST": true, "KUBERNETES_SERVICE_PORT": true,
}

var envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// JobBuilder assembles the batchv1.Job for a single TaskRun. Kept as a
// struct (not a bare function) so cluster-specific defaults — registry
// mirrors, default resource limits — can be set once at startup and reused
// across every Build call.
type JobBuilder struct {
	GitImage      string
	ArtifactImage string
	HelmImage     string
	KubectlImage  string
	// HubBaseURL is the hub API base reachable from job pods (e.g.
	// http://hub.sdp-workflow.svc:8080). Required for artifact upload
	// (archive scripts call the hub themselves) and consume downloads.
	HubBaseURL string
	// HubAPIToken is an optional bearer token for hub API calls from job
	// containers; empty is fine when the hub runs with auth disabled.
	HubAPIToken string
}

func NewJobBuilder() *JobBuilder {
	return &JobBuilder{
		GitImage:      DefaultGitImage,
		ArtifactImage: DefaultArtifactImage,
		HelmImage:     DefaultHelmImage,
		KubectlImage:  DefaultKubectlImage,
	}
}

// Build constructs the Job for tr. Callers are expected to
// controllerutil.SetControllerReference(tr, job, scheme) themselves so
// garbage collection is wired up — this package only knows about Jobs,
// not about the TaskRun controller's ownership bookkeeping.
func (b *JobBuilder) Build(tr *sdpv1alpha1.TaskRun) *batchv1.Job {
	var initContainers []corev1.Container

	if tr.Spec.Repo != nil {
		initContainers = append(initContainers, b.checkoutContainer(tr.Spec.Repo))
	}
	if len(tr.Spec.Consumes) > 0 {
		initContainers = append(initContainers, b.consumeContainer(tr.Spec.Consumes, tr.Spec.Params))
	}

	// A Release task with a canary sub-spec is owned by the Rollout
	// controller instead of a Job, so this builder never sees it. A plain
	// (non-canary) Release applies its chart/manifest via a Job, exactly
	// like a Build task — only the container command differs.
	var main corev1.Container
	if tr.Spec.Type == sdpv1alpha1.TaskTypeRelease && tr.Spec.RolloutSpec == nil {
		main = b.releaseContainer(tr)
	} else {
		main = b.mainContainer(tr)
	}

	backoffLimit := int32(0) // 重试由 TaskRun 控制器基于 RetryPolicy 重新创建 Job 决定,不用 Job 自带的重试

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			// Job 名字带上 TaskRun 名 + 一个短随机后缀由 controller-runtime 的
			// GenerateName 机制处理更合适,这里先用固定命名占位。
			Name:      "taskrun-" + tr.Name,
			Namespace: tr.Spec.Namespace,
			Labels: map[string]string{
				"sdp.io/pipeline-run": tr.Spec.PipelineRunRef,
				"sdp.io/task-run":     tr.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: tr.Spec.ServiceAccountName,
					InitContainers:     initContainers,
					Containers:         []corev1.Container{main},
					Volumes: []corev1.Volume{
						{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}

	if tr.Spec.TimeoutSeconds > 0 {
		job.Spec.ActiveDeadlineSeconds = &tr.Spec.TimeoutSeconds
	}

	return job
}

// checkoutContainer clones Repo.URL@Repo.Ref into the shared workspace
// volume. Private repos: SecretRef names a Secret already present in the
// target namespace, mounted read-only and referenced via GIT_ASKPASS —
// the runner never receives raw credentials from the hub over the wire.
func (b *JobBuilder) checkoutContainer(repo *sdpv1alpha1.RepoSource) corev1.Container {
	c := corev1.Container{
		Name:  "checkout",
		Image: b.GitImage,
		Command: []string{
			"sh", "-c",
			`git clone "$REPO_URL" "$WORKSPACE" && cd "$WORKSPACE" && git checkout "$REPO_REF"`,
		},
		Env: []corev1.EnvVar{
			{Name: "REPO_URL", Value: repo.URL},
			{Name: "REPO_REF", Value: repo.Ref},
			{Name: "WORKSPACE", Value: workspaceMountPath},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "workspace", MountPath: workspaceMountPath},
		},
	}
	if repo.SecretRef != "" {
		c.EnvFrom = []corev1.EnvFromSource{
			{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: repo.SecretRef}}},
		}
	}
	return c
}

// consumeFetchScript downloads every key in $ARTIFACT_KEYS (comma-separated)
// into $SDP_ARTIFACT_DIR via the hub's signed storage-URL endpoint (G-6).
// Consumed with a curl-capable image (default curlimages/curl).
const consumeFetchScript = `set -e
mkdir -p "$SDP_ARTIFACT_DIR"
OLDIFS="$IFS"; IFS=','
for key in $ARTIFACT_KEYS; do
  IFS="$OLDIFS"
  [ -n "$key" ] || continue
  if [ -n "$SDP_HUB_TOKEN" ]; then
    RESP=$(curl -sf -X POST "$SDP_HUB_BASE_URL/api/v1/artifacts/storage-url" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $SDP_HUB_TOKEN" \
      -d "{\"key\":\"$key\"}")
  else
    RESP=$(curl -sf -X POST "$SDP_HUB_BASE_URL/api/v1/artifacts/storage-url" \
      -H "Content-Type: application/json" \
      -d "{\"key\":\"$key\"}")
  fi
  [ -n "$RESP" ] || { echo "storage-url request failed for $key" >&2; exit 1; }
  URL=$(printf '%s' "$RESP" | sed -n 's/.*"url":"\([^"]*\)".*/\1/p')
  [ -n "$URL" ] || { echo "no url in response for $key: $RESP" >&2; exit 1; }
  OUT="$SDP_ARTIFACT_DIR/$(basename "$key")"
  curl -fsSL -o "$OUT" "$URL" || { echo "download failed for $key" >&2; exit 1; }
  echo "fetched $key -> $OUT ($(wc -c < "$OUT") bytes)"
done
`

// consumeContainer downloads every artifact key in Consumes from the object
// store into the workspace, so the main container can read them as plain
// local files (G-6: was an echo placeholder).
func (b *JobBuilder) consumeContainer(consumes []string, params []sdpv1alpha1.Param) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "ARTIFACT_KEYS", Value: joinComma(consumes)},
		{Name: "SDP_ARTIFACT_DIR", Value: workspaceMountPath},
		{Name: "SDP_HUB_BASE_URL", Value: b.HubBaseURL},
	}
	if b.HubAPIToken != "" {
		env = append(env, corev1.EnvVar{Name: "SDP_HUB_TOKEN", Value: b.HubAPIToken})
	}
	// Params can feed the download script too (e.g. ${KEY} already resolved
	// hub-side; plain $VERSION still works here).
	env = append(env, paramsToEnv(params)...)
	env = append(env, corev1.EnvVar{
		Name:  "AUTH_HEADER",
		Value: "",
	})
	if b.HubAPIToken != "" {
		env[len(env)-1] = corev1.EnvVar{Name: "AUTH_HEADER", Value: "-H \"Authorization: Bearer $SDP_HUB_TOKEN\""}
	}
	return corev1.Container{
		Name:    "fetch-artifacts",
		Image:   b.ArtifactImage,
		Command: []string{"sh", "-c", consumeFetchScript},
		Env:     env,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "workspace", MountPath: workspaceMountPath},
		},
	}
}

// paramsToEnv converts run params into container env vars (G-5, runner half).
// Only shell-safe names are injected; reserved names are skipped so a param
// can never clobber PATH/HOME or inject loader settings. The hub-side ${KEY}
// substitution covers arbitrary names.
func paramsToEnv(params []sdpv1alpha1.Param) []corev1.EnvVar {
	var env []corev1.EnvVar
	for _, p := range params {
		if p.Name == "" || !envNameRe.MatchString(p.Name) || reservedEnv[p.Name] {
			continue
		}
		env = append(env, corev1.EnvVar{Name: p.Name, Value: p.Value})
	}
	return env
}

// mainContainer runs `sh {ScriptPath} {ScriptArgs...}` from the workspace,
// with the standard SDP_* env vars injected so build.sh/run.sh/test.sh can
// read pipeline context without the platform having to template them into
// the command line.
func (b *JobBuilder) mainContainer(tr *sdpv1alpha1.TaskRun) corev1.Container {
	command := tr.Spec.Command
	args := tr.Spec.Args
	if tr.Spec.ScriptPath != "" {
		command = []string{"sh", tr.Spec.ScriptPath}
		args = tr.Spec.ScriptArgs
	}

	env := append([]corev1.EnvVar{
		{Name: "SDP_PIPELINE_RUN", Value: tr.Spec.PipelineRunRef},
		{Name: "SDP_TASK_NAME", Value: tr.Spec.TaskName},
		{Name: "SDP_ARTIFACT_DIR", Value: workspaceMountPath},
		{Name: "SDP_HUB_BASE_URL", Value: b.HubBaseURL},
	}, tr.Spec.Env...)
	env = append(env, paramsToEnv(tr.Spec.Params)...)

	c := corev1.Container{
		Name:         "main",
		Image:        tr.Spec.Image,
		Command:      command,
		Args:         args,
		Env:          env,
		Resources:    tr.Spec.Resources,
		WorkingDir:   workspaceMountPath,
		VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
	}
	applyPrivileged(&c, tr.Spec.Privileged)
	return c
}

// releaseContainer applies a software unit (Helm chart or raw manifest) into
// the target cluster. Values injected from the hub's parameter management are
// passed as `helm --set` pairs (chart) or an env var piped to `kubectl apply`
// (manifest). The release name defaults to the task name (sanitized); the
// runner never templates the chart itself — it only forwards the values.
// `${KEY}` placeholders were already expanded hub-side (G-5); run params are
// additionally injected as env for shell-level references.
func (b *JobBuilder) releaseContainer(tr *sdpv1alpha1.TaskRun) corev1.Container {
	spec := tr.Spec.ReleaseSpec
	if spec == nil {
		// Defensive: a Release task must carry a ReleaseSpec. Surface it as a
		// command that fails fast so the TaskRun records a clean Failed.
		return corev1.Container{
			Name:  "release",
			Image: b.HelmImage,
			Command: []string{"sh", "-c",
				"echo 'release task missing ReleaseSpec' >&2; exit 1"},
			WorkingDir:   workspaceMountPath,
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
		}
	}

	ns := spec.TargetNamespace
	if ns == "" {
		ns = tr.Spec.Namespace
	}
	releaseName := sanitizeReleaseName(tr.Spec.TaskName)

	switch spec.Source {
	case sdpv1alpha1.ReleaseSourceManifest:
		content := ""
		if spec.Manifest != nil {
			content = spec.Manifest.Content
		}
		env := []corev1.EnvVar{
			{Name: "SDP_MANIFEST", Value: content},
			{Name: "SDP_RELEASE_NS", Value: ns},
		}
		env = append(env, paramsToEnv(tr.Spec.Params)...)
		c := corev1.Container{
			Name:  "release",
			Image: firstNonEmpty(tr.Spec.Image, b.KubectlImage),
			Command: []string{"sh", "-c",
				`printf '%s' "$SDP_MANIFEST" | kubectl apply -f - --namespace "$SDP_RELEASE_NS"`},
			Env:          env,
			WorkingDir:   workspaceMountPath,
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
		}
		applyPrivileged(&c, tr.Spec.Privileged)
		return c
	default: // ReleaseSourceChart (and unspecified → chart)
		chart := ""
		if spec.Chart != nil {
			chart = spec.Chart.Name
		}
		cmd := []string{"helm", "upgrade", "--install", releaseName, chart}
		if spec.Chart != nil {
			if spec.Chart.RepoURL != "" {
				cmd = append(cmd, "--repo", spec.Chart.RepoURL)
			}
			if spec.Chart.Version != "" {
				cmd = append(cmd, "--version", spec.Chart.Version)
			}
		}
		if ns != "" {
			cmd = append(cmd, "--namespace", ns)
		}
		// Values injected from the hub's parameter management.
		for k, v := range spec.Values {
			cmd = append(cmd, "--set", k+"="+v)
		}
		c := corev1.Container{
			Name:         "release",
			Image:        firstNonEmpty(tr.Spec.Image, b.HelmImage),
			Command:      cmd,
			WorkingDir:   workspaceMountPath,
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
		}
		applyPrivileged(&c, tr.Spec.Privileged)
		return c
	}
}

// applyPrivileged opts the container into privileged mode (G-4) — required
// by docker-in-docker / containerd-in-containerd build images.
func applyPrivileged(c *corev1.Container, privileged bool) {
	if !privileged {
		return
	}
	priv := true
	c.SecurityContext = &corev1.SecurityContext{Privileged: &priv}
}

// sanitizeReleaseName makes a task name safe to use as a Helm release name
// ([a-z0-9]([-a-z0-9]*[a-z0-9])?, <=53 chars).
func sanitizeReleaseName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		case r == '-', r == '.':
			out = append(out, '-')
		default:
			out = append(out, '-')
		}
	}
	s := string(out)
	if s == "" {
		return "release"
	}
	if len(s) > 53 {
		s = s[:53]
	}
	// strip trailing non-alphanumeric
	for len(s) > 0 && (s[len(s)-1] == '-' || s[len(s)-1] == '.') {
		s = s[:len(s)-1]
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func joinComma(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}
