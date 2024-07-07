/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	proxyv1alpha1 "github.com/meoying/dbproxy/api/v1alpha1"
)

// DbproxyReconciler reconciles a Dbproxy object
type DbproxyReconciler struct {
	client.Client
	// 日志
	Log    logr.Logger
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=proxy.meoying.com,resources=dbproxies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=proxy.meoying.com,resources=dbproxies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=proxy.meoying.com,resources=dbproxies/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Dbproxy object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *DbproxyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	fmt.Println("xxxxxxxxxx", req)
	// TODO(user): your logic here
	// 获取 Dbproxy 实例
	var dbproxy proxyv1alpha1.Dbproxy
	err := r.Get(ctx, req.NamespacedName, &dbproxy)
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	// 如果不存在相关deployment资源就创建
	// 如果存在 判断是否要更新
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, req.NamespacedName, deploy); err != nil && errors.IsNotFound(err) {
		data, _ := json.Marshal(dbproxy.Spec)
		if dbproxy.ObjectMeta.Annotations != nil {
			dbproxy.ObjectMeta.Annotations["old"] = string(data)
		} else {
			dbproxy.ObjectMeta.Annotations = map[string]string{
				"old": string(data),
			}
		}
		// 创建关联资源
		deployWithDbproxy := r.NewDeployment(&dbproxy)
		if err = r.Create(ctx, deployWithDbproxy); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	oldAnno := dbproxy.ObjectMeta.Annotations["old"]
	oldSpec := proxyv1alpha1.DbproxySpec{}
	if err := json.Unmarshal([]byte(oldAnno), &oldSpec); err != nil {
		return ctrl.Result{}, err
	}
	// 和以前的不一样就更新
	if !reflect.DeepEqual(dbproxy.Spec, oldSpec) {
		// 更新关联资源
		newDeploy := r.NewDeployment(&dbproxy)
		oldDeploy := &appsv1.Deployment{}
		if err := r.Get(ctx, req.NamespacedName, oldDeploy); err != nil {
			return ctrl.Result{}, err
		}
		oldDeploy.Spec = newDeploy.Spec
		if err := r.Client.Update(ctx, oldDeploy); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DbproxyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&proxyv1alpha1.Dbproxy{}).
		Complete(r)
}

func (r *DbproxyReconciler) NewDeployment(app *proxyv1alpha1.Dbproxy) *appsv1.Deployment {
	// 往deployment的pod里面添加dbproxy容器
	dbContainer, volumns := r.NewDbproxyContainer(app)
	deploymentSpec := app.Spec
	// 将容器注入
	deploymentSpec.Template.Spec.Containers = append(deploymentSpec.Template.Spec.Containers, dbContainer)
	deploymentSpec.Template.Spec.Volumes = append(deploymentSpec.Template.Spec.Volumes, volumns...)
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      app.Name,
			Namespace: app.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, schema.GroupVersionKind{
					Group:   proxyv1alpha1.GroupVersion.Group,
					Version: proxyv1alpha1.GroupVersion.Version,
					Kind:    proxyv1alpha1.Kind,
				}),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: deploymentSpec.Replicas,
			Selector: deploymentSpec.Selector,
			Template: corev1.PodTemplateSpec{
				Spec:       deploymentSpec.Template.Spec,
				ObjectMeta: r.copyToMeta(deploymentSpec.Template.Meta),
			},
			Strategy:                deploymentSpec.Strategy,
			MinReadySeconds:         deploymentSpec.MinReadySeconds,
			RevisionHistoryLimit:    deploymentSpec.RevisionHistoryLimit,
			Paused:                  deploymentSpec.Paused,
			ProgressDeadlineSeconds: deploymentSpec.ProgressDeadlineSeconds,
		},
	}
}

func (r *DbproxyReconciler) NewDbproxyContainer(app *proxyv1alpha1.Dbproxy) (corev1.Container, []corev1.Volume) {
	container := corev1.Container{
		// 名称
		Name:  "dbproxy",
		Image: app.Spec.Image,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config-volume",
				MountPath: "/root/config",
			},
		},
	}
	volumns := []corev1.Volume{
		{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: app.Spec.Config,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  "config.yaml",
							Path: "config.yaml",
						},
					},
				},
			},
		},
	}

	for _, plugin := range app.Spec.Plugins {
		volumnName := fmt.Sprintf("%s-config-volume", plugin.Name)
		volumns = append(volumns, corev1.Volume{
			Name: volumnName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: plugin.Config,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  "config.yaml",
							Path: "config.yaml",
						},
					},
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volumnName,
			MountPath: fmt.Sprintf("/root/plugin/%s/config", plugin.Name),
		})
	}
	return container, volumns
}

func (r *DbproxyReconciler) copyToMeta(meta proxyv1alpha1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:                       meta.Name,
		GenerateName:               meta.GenerateName,
		Namespace:                  meta.Namespace,
		SelfLink:                   meta.SelfLink,
		UID:                        meta.UID,
		ResourceVersion:            meta.ResourceVersion,
		Generation:                 meta.Generation,
		CreationTimestamp:          meta.CreationTimestamp,
		DeletionTimestamp:          meta.DeletionTimestamp,
		DeletionGracePeriodSeconds: meta.DeletionGracePeriodSeconds,
		Labels:                     meta.Labels,
		Annotations:                meta.Annotations,
		OwnerReferences:            meta.OwnerReferences,
		Finalizers:                 meta.Finalizers,
		ManagedFields:              meta.ManagedFields,
	}
}
