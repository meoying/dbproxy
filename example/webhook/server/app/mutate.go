package app

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
)

type MutatingServer struct {
}

func (m *MutatingServer) RegisterHandler(router *gin.Engine) {
	router.POST("/mutate", m.MutateHandler)
}

func (m *MutatingServer) MutateHandler(c *gin.Context) {
	// 检查请求方法是否为 POST
	if c.Request.Method != http.MethodPost {
		c.String(http.StatusMethodNotAllowed, "Invalid method, only POST allowed")
		return
	}

	// 解析 AdmissionReview 对象
	var admissionReview v1.AdmissionReview

	if err := c.ShouldBindJSON(&admissionReview); err != nil {
		c.String(http.StatusBadRequest, "Error decoding admission review")
		return
	}

	// 处理 Mutating Webhook 请求并返回响应
	resp := mutatePod(admissionReview.Request)
	respAdmissionReview := v1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: resp,
	}
	c.JSON(http.StatusOK, respAdmissionReview)
}

func mutatePod(req *v1.AdmissionRequest) *v1.AdmissionResponse {
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return &v1.AdmissionResponse{
			Result: &metav1.Status{
				Message: err.Error(),
				Code:    http.StatusBadRequest,
			},
		}
	}
	// 添加 dbproxy 和 mysql 容器
	pod.Spec.Containers = addContainers(pod.Spec.Containers)
	// 添加volume
	pod.Spec.Volumes = addVolume(pod.Spec.Volumes)
	// 创建 AdmissionResponse

	containersByte, err := json.Marshal(pod.Spec.Containers)
	if err != nil {
		return &v1.AdmissionResponse{
			Result: &metav1.Status{
				Message: err.Error(),
				Code:    http.StatusInternalServerError,
			},
		}
	}
	volumnByte, err := json.Marshal(pod.Spec.Volumes)
	if err != nil {
		return &v1.AdmissionResponse{
			Result: &metav1.Status{
				Message: err.Error(),
				Code:    http.StatusInternalServerError,
			},
		}
	}
	// k8s操作json的方式参考RFC 6902对json做增删改
	patch := []JSONPatchEntry{
		{
			OP:    "replace",
			Path:  "/spec/containers",
			Value: containersByte,
		},
		{
			OP:    "replace",
			Path:  "/spec/volumes",
			Value: volumnByte,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return &v1.AdmissionResponse{
			Result: &metav1.Status{
				Message: err.Error(),
				Code:    http.StatusInternalServerError,
			},
		}
	}

	return &v1.AdmissionResponse{
		UID:     req.UID,
		Allowed: true,
		Patch:   patchBytes,
		PatchType: func() *v1.PatchType {
			pt := v1.PatchTypeJSONPatch
			return &pt
		}(),
	}
}

// 添加容器
func addContainers(containers []corev1.Container) []corev1.Container {
	// 添加 dbproxy 容器（示例中只添加名称，需要根据实际情况添加配置）
	dbproxyContainer := corev1.Container{
		Name:  "dbproxy",
		Image: "flycash/dbproxy:dbproxy-v0.1",
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config-volume",
				MountPath: "/root/config",
			},
			{
				Name:      "forward-plugin-volume",
				MountPath: "/root/plugin/forward/config",
			},
		},
	}

	containers = append([]corev1.Container{dbproxyContainer}, containers...)

	// 添加 mysql 容器
	mysqlContainer := corev1.Container{
		Name:  "mysql",
		Image: "mysql:8.0.29",
		Args: []string{
			"--default-authentication-plugin=mysql_native_password",
		},
		Env: []corev1.EnvVar{
			{
				Name:  "MYSQL_ROOT_PASSWORD",
				Value: "root",
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "mysql-init-script-volume",
				MountPath: "/docker-entrypoint-initdb.d/init.sql",
				SubPath:   "init.sql",
			},
		},
	}
	containers = append([]corev1.Container{mysqlContainer}, containers...)
	return containers
}

// 添加volume挂载项
func addVolume(volumes []corev1.Volume) []corev1.Volume {
	// 添加config-volume
	configVolume := corev1.Volume{
		Name: "config-volume",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "dbproxy-configmap",
				},
				Items: []corev1.KeyToPath{
					{
						Key:  "config.yaml",
						Path: "config.yaml",
					},
				},
			},
		},
	}
	forwardVolume := corev1.Volume{
		Name: "forward-plugin-volume",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "forward-plugin-configmap",
				},
				Items: []corev1.KeyToPath{
					{
						Key:  "config.yaml",
						Path: "config.yaml",
					},
				},
			},
		},
	}
	initVolume := corev1.Volume{
		Name: "mysql-init-script-volume",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "mysql-init-script-configmap",
				},
				Items: []corev1.KeyToPath{
					{
						Key:  "init.sql",
						Path: "init.sql",
					},
				},
			},
		},
	}
	volumes = append(volumes, configVolume, forwardVolume, initVolume)
	return volumes
}

type JSONPatchEntry struct {
	OP    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value,omitempty"`
}
