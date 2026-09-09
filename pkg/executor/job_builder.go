// Package executor turns a TaskRunSpec into a concrete Kubernetes Job —
// this is where the design discussed for script-based task execution
// (git checkout + artifact download as init containers, `sh {ScriptPath}
// {ScriptArgs...}` as the main container) actually gets built.
package executor

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

const workspaceMountPath = "/workspace"

// standard images used for the plumbing init containers; overridable via
// JobBuilder fields so a self-hosted registry mirror can be swapped in
// without touching call sites.
const (
	defaultGitImage      = "alpine/git:2.45.2"
	defaultArtifactImage = "amazon/aws-cli:2.17.0" // 占位:换成实际的对象存储 CLI/自研小工具镜像

	// defaultReleaseImage* are placeholders; swap for the org's pinned
	// helm/kubectl images (or a self-built release tool) before production.
	defaultHelmImage    = "alpine/helm:3.14.4"
	defaultKubectlImage = "bitnami/kubectl:1.30" // 占位:换成实际 kubectl 镜像
)

// JobBuilder assembles the batchv1.Job for a single TaskRun. Kept as a
// struct (not a bare function) so cluster-specific defaults — registry
// mirrors, default resource limits — can be set once at startup and reused
// across every Build call.
type JobBuilder struct {
	GitImage      string
	ArtifactImage string
	// ArtifactStoreEndpoint is passed to the artifact-download init
	// container via env var; TODO: 换成真正的对象存储 SDK 调用方式。
	ArtifactStoreEndpoint string
}

func NewJobBuilder() *JobBuilder {
	return &JobBuilder{GitImage: defaultGitImage, ArtifactImage: defaultArtifactImage}
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
		initContainers = append(initContainers, b.consumeContainer(tr.Spec.Consumes))
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

// consumeContainer downloads every artifact key in Consumes from the
// central object store into the workspace, so ScriptPath can read them as
// plain local files. TODO: 换成真正的对象存储 CLI/自研小工具镜像和调用方式,
// 这里先用环境变量把要拉取的 key 列表传进去占位。
func (b *JobBuilder) consumeContainer(consumes []string) corev1.Container {
	return corev1.Container{
		Name:    "fetch-artifacts",
		Image:   b.ArtifactImage,
		Command: []string{"sh", "-c", "echo fetching artifacts: $ARTIFACT_KEYS"},
		Env: []corev1.EnvVar{
			{Name: "ARTIFACT_KEYS", Value: joinComma(consumes)},
			{Name: "ARTIFACT_STORE_ENDPOINT", Value: b.ArtifactStoreEndpoint},
			{Name: "SDP_ARTIFACT_DIR", Value: workspaceMountPath},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "workspace", MountPath: workspaceMountPath},
		},
	}
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
	}, tr.Spec.Env...)

	return corev1.Container{
		Name:         "main",
		Image:        tr.Spec.Image,
		Command:      command,
		Args:         args,
		Env:          env,
		Resources:    tr.Spec.Resources,
		WorkingDir:   workspaceMountPath,
		VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
	}
}

// releaseContainer applies a software unit (Helm chart or raw manifest) into
// the target cluster. Values injected from the hub's parameter management are
// passed as `helm --set` pairs (chart) or an env var piped to `kubectl apply`
// (manifest). The release name defaults to the task name (sanitized); the
// runner never templates the chart itself — it only forwards the values.
func (b *JobBuilder) releaseContainer(tr *sdpv1alpha1.TaskRun) corev1.Container {
	spec := tr.Spec.ReleaseSpec
	if spec == nil {
		// Defensive: a Release task must carry a ReleaseSpec. Surface it as a
		// command that fails fast so the TaskRun records a clean Failed.
		return corev1.Container{
			Name:  "release",
			Image: defaultHelmImage,
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
		return corev1.Container{
			Name:  "release",
			Image: firstNonEmpty(tr.Spec.Image, defaultKubectlImage),
			Command: []string{"sh", "-c",
				`printf '%s' "$SDP_MANIFEST" | kubectl apply -f - --namespace "$SDP_RELEASE_NS"`},
			Env: []corev1.EnvVar{
				{Name: "SDP_MANIFEST", Value: content},
				{Name: "SDP_RELEASE_NS", Value: ns},
			},
			WorkingDir:   workspaceMountPath,
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
		}
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
		return corev1.Container{
			Name:         "release",
			Image:        firstNonEmpty(tr.Spec.Image, defaultHelmImage),
			Command:      cmd,
			WorkingDir:   workspaceMountPath,
			VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: workspaceMountPath}},
		}
	}
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
