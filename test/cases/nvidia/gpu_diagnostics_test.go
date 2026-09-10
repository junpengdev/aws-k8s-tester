//go:build e2e

package nvidia

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

// Diagnostics for a device-plugin rollout that does not come up. Every function
// here takes the sharingFeature so the output names the feature that is actually
// running and inspects that feature's ConfigMap; hardcoding either meant an MPS
// failure reported itself as time-slicing and checked the wrong ConfigMap.

// watchDevicePluginRollout logs DaemonSet counters and pod state every 15s while the
// caller waits, so a failure comes with a timeline instead of one snapshot. A single
// end-state snapshot cannot distinguish "never started" from "started and died" from
// "nearly ready"; the progression can. The returned function stops it.
func watchDevicePluginRollout(ctx context.Context, cfg *envconf.Config, f sharingFeature) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		res := cfg.Client().Resources()
		started := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				el := time.Since(started).Round(time.Second)
				var ds appsv1.DaemonSet
				if err := res.Get(ctx, devicePluginName, devicePluginNamespace, &ds); err != nil {
					log.Printf("[%s +%s] DaemonSet not readable: %v", f.name, el, err)
					continue
				}
				st := ds.Status
				var pods corev1.PodList
				desc := "no pods"
				if err := res.List(ctx, &pods, resources.WithLabelSelector(devicePluginPodSelector)); err == nil {
					parts := []string{}
					for _, p := range pods.Items {
						if p.Namespace != devicePluginNamespace {
							continue
						}
						reason := string(p.Status.Phase)
						for _, cs := range p.Status.ContainerStatuses {
							if w := cs.State.Waiting; w != nil {
								reason = w.Reason
							}
						}
						parts = append(parts, fmt.Sprintf("%s=%s", p.Name, reason))
					}
					if len(parts) > 0 {
						desc = strings.Join(parts, " ")
					}
				}
				log.Printf("[%s +%s] desired=%d ready=%d unavailable=%d | %s",
					f.name, el, st.DesiredNumberScheduled, st.NumberReady, st.NumberUnavailable, desc)
			}
		}
	}()
	return func() { cancel(); <-done }
}

// dumpDevicePluginDiagnostics prints everything needed to explain a readiness
// failure: the DaemonSet's own counters, each pod's phase and container waiting
// reason, recent events, and any container output.
//
// Without this a failure is just "did not become ready", which is what the first two
// runs of the time-slicing test produced -- enough to know it broke, not enough to
// know why. fwext.ApplyManifests cannot be relied on to surface the cause either,
// because processObjects discards per-object Create errors (internal/e2e/client.go),
// so a missing ConfigMap looks identical to a crashlooping container from the
// caller's side.
func dumpDevicePluginDiagnostics(ctx context.Context, cfg *envconf.Config, f sharingFeature) {
	res := cfg.Client().Resources()

	// Did the ConfigMap the pod mounts actually get created? A swallowed Create
	// error here leaves the pod stuck in ContainerCreating indefinitely.
	var cm corev1.ConfigMap
	if err := res.Get(ctx, f.configMapName, devicePluginNamespace, &cm); err != nil {
		log.Printf("[%s][diag] ConfigMap %s/%s NOT FOUND: %v -- the pod cannot mount its config",
			f.name, devicePluginNamespace, f.configMapName, err)
	} else {
		log.Printf("[%s][diag] ConfigMap %s/%s exists, keys=%v",
			f.name, devicePluginNamespace, f.configMapName, mapKeys(cm.Data))
	}

	var ds appsv1.DaemonSet
	if err := res.Get(ctx, devicePluginName, devicePluginNamespace, &ds); err != nil {
		log.Printf("[%s][diag] DaemonSet not readable: %v", f.name, err)
	} else {
		st := ds.Status
		log.Printf("[%s][diag] DaemonSet desired=%d current=%d ready=%d available=%d unavailable=%d misscheduled=%d",
			f.name, st.DesiredNumberScheduled, st.CurrentNumberScheduled, st.NumberReady,
			st.NumberAvailable, st.NumberUnavailable, st.NumberMisscheduled)
	}

	var pods corev1.PodList
	if err := res.List(ctx, &pods, resources.WithLabelSelector(devicePluginPodSelector)); err != nil {
		log.Printf("[%s][diag] could not list plugin pods: %v", f.name, err)
		return
	}
	found := 0
	for _, pod := range pods.Items {
		if pod.Namespace != devicePluginNamespace {
			continue
		}
		found++
		log.Printf("[%s][diag] pod %s phase=%s node=%s", f.name, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
		for _, c := range pod.Status.Conditions {
			log.Printf("[%s][diag]   condition %s=%s %s %s", f.name, c.Type, c.Status, c.Reason, c.Message)
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if w := cs.State.Waiting; w != nil {
				log.Printf("[%s][diag]   container %s WAITING reason=%s message=%s", f.name, cs.Name, w.Reason, w.Message)
			}
			if tm := cs.State.Terminated; tm != nil {
				log.Printf("[%s][diag]   container %s TERMINATED exit=%d reason=%s", f.name, cs.Name, tm.ExitCode, tm.Reason)
			}
			if cs.RestartCount > 0 {
				log.Printf("[%s][diag]   container %s restarts=%d", f.name, cs.Name, cs.RestartCount)
			}
		}
		if out := podLogs(ctx, cfg, pod.Namespace, pod.Name, false); out != "" {
			log.Printf("[%s][diag] pod %s logs:\n%s", f.name, pod.Name, out)
		}
		// A crashlooping container has no current logs; the useful output is from
		// the instance that already died.
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.RestartCount > 0 {
				if out := podLogs(ctx, cfg, pod.Namespace, pod.Name, true); out != "" {
					log.Printf("[%s][diag] pod %s PREVIOUS logs:\n%s", f.name, pod.Name, out)
				}
				break
			}
		}
		// If it never got scheduled, the reason is on the node, not the pod.
		if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodPending {
			dumpNodeState(ctx, cfg, f)
		}
	}
	if found == 0 {
		log.Printf("[%s][diag] NO pods matched %q in %s -- the DaemonSet controller created none, so look at the DaemonSet events above",
			f.name, devicePluginPodSelector, devicePluginNamespace)
	}

	// Include Normal events, not just Warnings: Scheduled / Pulling / Pulled /
	// Created / Started are precisely what show how far the pod got before it
	// stalled, and their absence is as informative as their content.
	var events corev1.EventList
	if err := res.List(ctx, &events); err == nil {
		for _, e := range events.Items {
			if e.Namespace != devicePluginNamespace {
				continue
			}
			if !strings.Contains(e.InvolvedObject.Name, "nvidia-device-plugin") {
				continue
			}
			log.Printf("[%s][diag] event %s %s/%s %s: %s",
				f.name, e.Type, e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Reason, e.Message)
		}
	}
}

// dumpNodeState explains a Pending pod: taints it may not tolerate, and whether the
// node is actually schedulable.
func dumpNodeState(ctx context.Context, cfg *envconf.Config, f sharingFeature) {
	var nodes corev1.NodeList
	if err := cfg.Client().Resources().List(ctx, &nodes); err != nil {
		return
	}
	for _, n := range nodes.Items {
		gpuQ := n.Status.Allocatable["nvidia.com/gpu"]
		log.Printf("[%s][diag] node %s unschedulable=%v allocatable-gpu=%d",
			f.name, n.Name, n.Spec.Unschedulable, gpuQ.Value())
		for _, tn := range n.Spec.Taints {
			log.Printf("[%s][diag]   taint %s=%s:%s", f.name, tn.Key, tn.Value, tn.Effect)
		}
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady || c.Status != corev1.ConditionFalse {
				log.Printf("[%s][diag]   condition %s=%s %s", f.name, c.Type, c.Status, c.Reason)
			}
		}
	}
}
