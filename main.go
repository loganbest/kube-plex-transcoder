package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/loganbest/kube-plex-transcoder/pkg/signals"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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

	cfg, err := clientcmd.BuildConfigFromFlags("", "")
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
	slog.Info("pod created", "pod", pod.Name, "namespace", namespace)

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
				Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
			},
		},
	}
}

func generatePod(cwd string, env []string, args []string) *corev1.Pod {
	envVars := toCoreV1EnvVar(env)
	return &corev1.Pod{
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
			},
		},
	}
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
			return fmt.Errorf("pod %q failed", pod.Name)
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
