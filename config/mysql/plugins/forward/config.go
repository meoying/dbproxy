package forward

// Config datasource为单个db
type Config struct {
	Dsn    string `json:"dsn" yaml:"dsn"`
	DBName string `json:"name" yaml:"name"`
}
