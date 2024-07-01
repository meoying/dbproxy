module github.com/meoying/dbproxy

go 1.22.0

require (
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/antlr4-go/antlr/v4 v4.13.0
	github.com/ecodeclub/ekit v0.0.9-0.20240604015119-6fdf3ad42c4b
	github.com/go-sql-driver/mysql v1.8.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/magiconair/properties v1.8.7
	github.com/mattn/go-sqlite3 v1.14.15
	github.com/pkg/errors v0.9.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.18.2
	github.com/stretchr/testify v1.9.0
	go.uber.org/mock v0.4.0
	go.uber.org/multierr v1.11.0
	golang.org/x/sync v0.7.0
	k8s.io/api v0.30.2
	k8s.io/apimachinery v0.30.2
	k8s.io/klog v1.0.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.120.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
)

replace github.com/antlr4-go/antlr/v4 => ./antlr
