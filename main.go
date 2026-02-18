package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/loganbest/kube-plex-transcoder/pkg/signals"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// data pvc name
var dataPVC = os.Getenv("DATA_PVC")

// config pvc name
var configPVC = os.Getenv("CONFIG_PVC")

// transcode pvc name (preferred)
var transcodePVC = os.Getenv("TRANSCODE_PVC")

// transcode hostPath directory (fallback when TRANSCODE_PVC is not set)
var transcodeDir = os.Getenv("TRANSCODE_DIR")

// pms namespace
var namespace = os.Getenv("KUBE_NAMESPACE")

// image for the plexmediaserver container containing the transcoder. This
// should be set to the same as the 'master' pms server
var pmsImage = os.Getenv("PMS_IMAGE")
var pmsInternalAddress = os.Getenv("PMS_INTERNAL_ADDRESS")

func main() {
	env := os.Environ()
	args := os.Args

	if err := validateRequiredEnv(); err != nil {
		slog.Error("missing required environment variable", "err", err)
		os.Exit(1)
	}

	rewriteEnv(env)
	rewriteArgs(args)
	cwd, err := os.Getwd()
	if err != nil {
		slog.Error("failed to get working directory", "err", err)
		os.Exit(1)
	}
	pod := generatePod(cwd, env, args)

	cfg, err := getKubeConfig()
	if err != nil {
		slog.Error("failed to build kubeconfig", "err", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("failed to build kubernetes clientset", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopCh := signals.SetupSignalHandler()
	go func() {
		<-stopCh
		cancel()
	}()

	pod, err = kubeClient.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		slog.Error("failed to create pod", "err", err)
		os.Exit(1)
	}
	cmd := pod.Spec.Containers[0].Command
	slog.Info("pod created",
		"pod", pod.Name,
		"namespace", namespace,
		"image", pmsImage,
		"command", cmd,
	)

	doneCh := make(chan error)
	go func() {
		doneCh <- waitForPodCompletion(ctx, kubeClient, pod)
	}()

	select {
	case err := <-doneCh:
		if err != nil {
			slog.Error("pod failed or error waiting", "pod", pod.Name, "err", err)
		}
	case <-stopCh:
		slog.Info("exit requested")
	}

	slog.Info("cleaning up pod", "pod", pod.Name)
	// Use a fresh context for cleanup so Delete succeeds even after shutdown signal (ctx is cancelled)
	cleanupCtx := context.WithoutCancel(ctx)
	err = kubeClient.CoreV1().Pods(pod.Namespace).Delete(cleanupCtx, pod.Name, metav1.DeleteOptions{})
	if err != nil {
		slog.Error("failed to delete pod", "pod", pod.Name, "err", err)
		os.Exit(1)
	}
}

func getKubeConfig() (*rest.Config, error) {
	// Try in-cluster config first (when running inside a pod) - avoids clientcmd warning
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	// Fall back to kubeconfig for local dev or when KUBECONFIG is set
	return clientcmd.BuildConfigFromFlags("", "")
}

func validateRequiredEnv() error {
	required := []struct {
		name string
		val  string
	}{
		{"DATA_PVC", dataPVC},
		{"CONFIG_PVC", configPVC},
		{"KUBE_NAMESPACE", namespace},
		{"PMS_IMAGE", pmsImage},
	}
	for _, r := range required {
		if r.val == "" {
			return fmt.Errorf("%s must be set", r.name)
		}
	}
	if transcodePVC == "" && transcodeDir == "" {
		return fmt.Errorf("either TRANSCODE_PVC or TRANSCODE_DIR must be set")
	}
	return nil
}

// rewriteEnv rewrites environment variables to be passed to the transcoder
func rewriteEnv(in []string) {
	// no changes needed
}

func rewriteArgs(in []string) {
	for i, v := range in {
		if i+1 >= len(in) {
			continue
		}
		switch v {
		case "-progressurl", "-manifest_name", "-segment_list":
			in[i+1] = strings.Replace(in[i+1], "http://127.0.0.1:32400", pmsInternalAddress, 1)
		case "-loglevel", "-loglevel_plex":
			in[i+1] = "debug"
		}
	}
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType {
	return &t
}

func ptr[T any](v T) *T {
	return &v
}

func transcodeVolume() corev1.Volume {
	if transcodePVC != "" {
		return corev1.Volume{
			Name: "transcode",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: transcodePVC,
				},
			},
		}
	}
	return corev1.Volume{
		Name: "transcode",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: transcodeDir,
			},
		},
	}
}

func generatePod(cwd string, env []string, args []string) *corev1.Pod {
	envVars := toCoreV1EnvVar(env)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pms-elastic-transcoder-",
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/arch": "amd64",
			},
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:       "plex",
					Command:    args,
					Image:      pmsImage,
					Env:        envVars,
					WorkingDir: cwd,
					SecurityContext: &corev1.SecurityContext{
						Privileged: ptr(true),
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/media",
							ReadOnly:  true,
						},
						{
							Name:      "config",
							MountPath: "/config",
							ReadOnly:  true,
						},
						{
							Name:      "transcode",
							MountPath: "/transcode",
						},
						// EAE (Easy Audio Encoder) hardcodes /tmp and ignores TMPDIR. Mount transcode at /tmp
						// so EAE watchfolder writes succeed. With hostPath, transcoder must run on same node as PMS.
						// Set NODE_NAME via downward API in your PMS deployment to enable same-node scheduling.
						{
							Name:      "transcode",
							MountPath: "/tmp",
						},
						{
							Name:      "dri",
							MountPath: "/dev/dri",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: dataPVC,
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: configPVC,
						},
					},
				},
				transcodeVolume(),
				{
					Name: "dri",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/dev/dri",
							Type: hostPathTypePtr(corev1.HostPathDirectory),
						},
					},
				},
			},
		},
	}
	// With hostPath transcode, transcoder must run on same node as PMS for shared /tmp.
	// Set NODE_NAME via downward API (fieldRef: spec.nodeName) in your PMS deployment.
	if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{nodeName},
								},
							},
						},
					},
				},
			},
		}
	}
	return pod
}

func toCoreV1EnvVar(in []string) []corev1.EnvVar {
	var out []corev1.EnvVar
	for _, v := range in {
		splitvar := strings.SplitN(v, "=", 2)
		if len(splitvar) != 2 {
			slog.Warn("skipping malformed env var (no = separator)")
			continue
		}
		out = append(out, corev1.EnvVar{
			Name:  splitvar[0],
			Value: splitvar[1],
		})
	}
	return out
}

func logPodLogs(ctx context.Context, cl kubernetes.Interface, pod *corev1.Pod) {
	tailLines := int64(200)
	for _, c := range pod.Spec.Containers {
		opts := &corev1.PodLogOptions{
			Container: c.Name,
			TailLines: &tailLines,
		}
		req := cl.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts)
		stream, err := req.Stream(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "forbidden") || strings.Contains(err.Error(), "cannot get") {
				slog.Warn("cannot fetch pod logs: service account needs pods/log permission",
					"pod", pod.Name, "container", c.Name, "hint", "see deploy/rbac-pod-logs.yaml")
			} else {
				slog.Warn("failed to get pod logs", "pod", pod.Name, "container", c.Name, "err", err)
			}
			continue
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, stream); err != nil {
			stream.Close()
			slog.Warn("failed to read pod logs", "pod", pod.Name, "container", c.Name, "err", err)
			continue
		}
		stream.Close()
		logs := strings.TrimSpace(buf.String())
		if logs != "" {
			if summary := extractLogSummary(logs); summary != "" {
				slog.Info("pod container logs (errors/warnings)", "pod", pod.Name, "container", c.Name, "summary", summary)
			}
			slog.Info("pod container logs (full)", "pod", pod.Name, "container", c.Name, "logs", logs)
		}
	}
}

// extractLogSummary extracts error/warning lines from Plex Transcoder logs.
// Plex sends FFmpeg output via POST URLs with message= param; we decode and filter.
var messageParamRE = regexp.MustCompile(`message=([^&\s]+)`)

func extractLogSummary(logs string) string {
	var lines []string
	seen := make(map[string]bool)
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "message=") {
			continue
		}
		matches := messageParamRE.FindStringSubmatch(line)
		if len(matches) < 2 {
			continue
		}
		decoded, err := url.QueryUnescape(matches[1])
		if err != nil {
			continue
		}
		// Keep errors and warnings (level=0 or level=3), skip verbose (level=4)
		if strings.Contains(line, "level=4&message=") {
			continue
		}
		lower := strings.ToLower(decoded)
		if strings.Contains(lower, "error") || strings.Contains(lower, "failed") ||
			strings.Contains(lower, "warning") || strings.Contains(lower, "invalid") {
			if !seen[decoded] {
				seen[decoded] = true
				lines = append(lines, decoded)
			}
		}
	}
	return strings.Join(lines, "; ")
}

func formatPodFailureReason(pod *corev1.Pod) string {
	var parts []string
	if pod.Status.Reason != "" {
		parts = append(parts, fmt.Sprintf("reason=%s", pod.Status.Reason))
	}
	if pod.Status.Message != "" {
		parts = append(parts, fmt.Sprintf("message=%s", pod.Status.Message))
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			t := cs.State.Terminated
			parts = append(parts, fmt.Sprintf("container=%s exitCode=%d reason=%s",
				cs.Name, t.ExitCode, t.Reason))
			if t.Message != "" {
				parts = append(parts, fmt.Sprintf("containerMsg=%s", t.Message))
			}
		}
		if cs.State.Waiting != nil {
			w := cs.State.Waiting
			parts = append(parts, fmt.Sprintf("container=%s waiting reason=%s",
				cs.Name, w.Reason))
			if w.Message != "" {
				parts = append(parts, fmt.Sprintf("waitingMsg=%s", w.Message))
			}
		}
		if cs.LastTerminationState.Terminated != nil {
			t := cs.LastTerminationState.Terminated
			parts = append(parts, fmt.Sprintf("lastExit=%d lastReason=%s", t.ExitCode, t.Reason))
		}
	}
	if len(parts) == 0 {
		return "no status details available"
	}
	return strings.Join(parts, "; ")
}

func waitForPodCompletion(ctx context.Context, cl kubernetes.Interface, pod *corev1.Pod) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		pod, err := cl.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		switch pod.Status.Phase {
		case corev1.PodPending:
		case corev1.PodRunning:
		case corev1.PodUnknown:
			slog.Warn("pod in unknown state", "pod", pod.Name)
		case corev1.PodFailed:
			logPodLogs(ctx, cl, pod)
			return fmt.Errorf("pod %q failed: %s", pod.Name, formatPodFailureReason(pod))
		case corev1.PodSucceeded:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
