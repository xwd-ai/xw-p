package domain

// Environment 是隔离环境，与 Python 侧 Environment 对齐。
// 同一套实体在不同环境间数据隔离，API Key 按环境区分。
type Environment string

const (
	// EnvDevelopment 开发环境。
	EnvDevelopment Environment = "development"
	// EnvProduction 生产环境。
	EnvProduction Environment = "production"
)

// AllEnvironments 返回全部环境。
func AllEnvironments() []Environment {
	return []Environment{EnvDevelopment, EnvProduction}
}

// ParseEnvironment 按存储值解析环境；未知值返回 false。
func ParseEnvironment(value string) (Environment, bool) {
	for _, e := range AllEnvironments() {
		if string(e) == value {
			return e, true
		}
	}
	return "", false
}
