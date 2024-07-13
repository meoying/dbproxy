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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DbproxySpec defines the desired state of Dbproxy
type DbproxySpec struct {
	// 你的app设置
	Replicas                *int32                    `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
	Selector                *metav1.LabelSelector     `json:"selector" protobuf:"bytes,2,opt,name=selector"`
	Template                PodTemplateSpec           `json:"template" protobuf:"bytes,3,opt,name=template"`
	Strategy                appsv1.DeploymentStrategy `json:"strategy,omitempty" patchStrategy:"retainKeys" protobuf:"bytes,4,opt,name=strategy"`
	MinReadySeconds         int32                     `json:"minReadySeconds,omitempty" protobuf:"varint,5,opt,name=minReadySeconds"`
	RevisionHistoryLimit    *int32                    `json:"revisionHistoryLimit,omitempty" protobuf:"varint,6,opt,name=revisionHistoryLimit"`
	Paused                  bool                      `json:"paused,omitempty" protobuf:"varint,7,opt,name=paused"`
	ProgressDeadlineSeconds *int32                    `json:"progressDeadlineSeconds,omitempty" protobuf:"varint,9,opt,name=progressDeadlineSeconds"`
	// dbproxy的配置
	Image   string         `json:"image"`
	Plugins []PluginConfig `json:"plugins"`
	Config  string         `json:"config"`
}

type PodTemplateSpec struct {
	Meta ObjectMeta     `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Spec corev1.PodSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

type ObjectMeta struct {
	Name                       string                      `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	GenerateName               string                      `json:"generateName,omitempty" protobuf:"bytes,2,opt,name=generateName"`
	Namespace                  string                      `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`
	SelfLink                   string                      `json:"selfLink,omitempty" protobuf:"bytes,4,opt,name=selfLink"`
	UID                        types.UID                   `json:"uid,omitempty" protobuf:"bytes,5,opt,name=uid,casttype=k8s.io/kubernetes/pkg/types.UID"`
	ResourceVersion            string                      `json:"resourceVersion,omitempty" protobuf:"bytes,6,opt,name=resourceVersion"`
	Generation                 int64                       `json:"generation,omitempty" protobuf:"varint,7,opt,name=generation"`
	CreationTimestamp          metav1.Time                 `json:"creationTimestamp,omitempty" protobuf:"bytes,8,opt,name=creationTimestamp"`
	DeletionTimestamp          *metav1.Time                `json:"deletionTimestamp,omitempty" protobuf:"bytes,9,opt,name=deletionTimestamp"`
	DeletionGracePeriodSeconds *int64                      `json:"deletionGracePeriodSeconds,omitempty" protobuf:"varint,10,opt,name=deletionGracePeriodSeconds"`
	Labels                     map[string]string           `json:"labels,omitempty" protobuf:"bytes,11,rep,name=labels"`
	Annotations                map[string]string           `json:"annotations,omitempty" protobuf:"bytes,12,rep,name=annotations"`
	OwnerReferences            []metav1.OwnerReference     `json:"ownerReferences,omitempty" patchStrategy:"merge" patchMergeKey:"uid" protobuf:"bytes,13,rep,name=ownerReferences"`
	Finalizers                 []string                    `json:"finalizers,omitempty" patchStrategy:"merge" protobuf:"bytes,14,rep,name=finalizers"`
	ManagedFields              []metav1.ManagedFieldsEntry `json:"managedFields,omitempty" protobuf:"bytes,17,rep,name=managedFields"`
}

type PluginConfig struct {
	Name   string `json:"name"`
	Config string `json:"config"`
}

// DbproxyStatus defines the observed state of Dbproxy
type DbproxyStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 偷个懒直接使用deployment的状态
	appsv1.DeploymentStatus `json:",inline"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Dbproxy is the Schema for the dbproxies API
type Dbproxy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DbproxySpec   `json:"spec,omitempty"`
	Status DbproxyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DbproxyList contains a list of Dbproxy
type DbproxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dbproxy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Dbproxy{}, &DbproxyList{})
}
