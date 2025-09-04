package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Environment represents the application environment
type Environment string

const (
	EnvDevelopment Environment = "dev"
	EnvTest        Environment = "test"
	EnvProduction  Environment = "prod"
)

// String returns the string representation of the environment
func (e Environment) String() string {
	return string(e)
}

// IsValid checks if the environment is valid
func (e Environment) IsValid() bool {
	return e == EnvDevelopment || e == EnvTest || e == EnvProduction
}

// EnvironmentDetector defines the interface for environment detection
type EnvironmentDetector interface {
	Detect() (env Environment, confidence int, err error)
}

// EnvironmentDetectorRegistry manages multiple environment detectors
type EnvironmentDetectorRegistry struct {
	detectors []EnvironmentDetector
}

// NewEnvironmentDetectorRegistry creates a new detector registry with default detectors
func NewEnvironmentDetectorRegistry() *EnvironmentDetectorRegistry {
	return &EnvironmentDetectorRegistry{
		detectors: []EnvironmentDetector{
			&CommandLineEnvDetector{},
			&EnvVarDetector{},
			&FileExistenceDetector{},
			&RuntimeEnvDetector{},
		},
	}
}

// DetectEnvironment detects the current environment using all registered detectors
func (r *EnvironmentDetectorRegistry) DetectEnvironment() Environment {
	bestEnv := EnvDevelopment // default environment
	bestConfidence := 0

	for _, detector := range r.detectors {
		env, confidence, err := detector.Detect()
		if err != nil {
			continue // skip failed detectors
		}
		if confidence > bestConfidence {
			bestEnv = env
			bestConfidence = confidence
		}
	}

	return bestEnv
}

// CommandLineEnvDetector detects environment from command line arguments
type CommandLineEnvDetector struct{}

// Detect implements EnvironmentDetector interface
func (d *CommandLineEnvDetector) Detect() (Environment, int, error) {
	// Check if -env flag is set in command line arguments
	for i, arg := range os.Args {
		if strings.HasPrefix(arg, "-env=") {
			envStr := strings.TrimPrefix(arg, "-env=")
			env := Environment(envStr)
			if env.IsValid() {
				return env, 100, nil // highest confidence
			}
			return "", 0, fmt.Errorf("invalid environment: %s", envStr)
		}
		// Check for separate -env flag
		if arg == "-env" && i+1 < len(os.Args) {
			envStr := os.Args[i+1]
			env := Environment(envStr)
			if env.IsValid() {
				return env, 100, nil
			}
			return "", 0, fmt.Errorf("invalid environment: %s", envStr)
		}
	}
	return "", 0, nil // no command line environment specified
}

// EnvVarDetector detects environment from environment variables
type EnvVarDetector struct{}

// Detect implements EnvironmentDetector interface
func (d *EnvVarDetector) Detect() (Environment, int, error) {
	envStr := os.Getenv("BDL_ENV")
	if envStr == "" {
		return "", 0, nil // no environment variable set
	}

	env := Environment(envStr)
	if env.IsValid() {
		return env, 90, nil // high confidence
	}
	return "", 0, fmt.Errorf("invalid environment in BDL_ENV: %s", envStr)
}

// FileExistenceDetector detects environment based on config file existence
type FileExistenceDetector struct{}

// Detect implements EnvironmentDetector interface
func (d *FileExistenceDetector) Detect() (Environment, int, error) {
	// Check for environment-specific config files in order of preference
	envs := []Environment{EnvProduction, EnvTest, EnvDevelopment}
	paths := []string{"./configs", "../configs", "."}

	for _, env := range envs {
		for _, path := range paths {
			configFile := filepath.Join(path, fmt.Sprintf("config-%s.yml", env))
			if _, err := os.Stat(configFile); err == nil {
				return env, 70, nil // medium confidence
			}
		}
	}

	return "", 0, nil // no environment-specific config files found
}

// RuntimeEnvDetector detects environment based on runtime characteristics
type RuntimeEnvDetector struct{}

// Detect implements EnvironmentDetector interface
func (d *RuntimeEnvDetector) Detect() (Environment, int, error) {
	// Check for container environment
	if d.isInContainer() {
		return EnvProduction, 50, nil
	}

	// Check for CI/CD environment
	if d.isInCI() {
		return EnvTest, 50, nil
	}

	// Check for development indicators
	if d.isInDevelopment() {
		return EnvDevelopment, 30, nil
	}

	return "", 0, nil // unable to determine from runtime
}

// isInContainer checks if running in a container
func (d *RuntimeEnvDetector) isInContainer() bool {
	// Check for container-specific files
	containerFiles := []string{
		"/.dockerenv",
		"/proc/1/cgroup",
	}

	for _, file := range containerFiles {
		if _, err := os.Stat(file); err == nil {
			return true
		}
	}

	// Check container environment variables
	containerEnvs := []string{
		"DOCKER_CONTAINER",
		"KUBERNETES_SERVICE_HOST",
		"CONTAINER",
	}

	for _, env := range containerEnvs {
		if os.Getenv(env) != "" {
			return true
		}
	}

	return false
}

// isInCI checks if running in CI/CD environment
func (d *RuntimeEnvDetector) isInCI() bool {
	ciEnvs := []string{
		"CI",
		"CONTINUOUS_INTEGRATION",
		"GITHUB_ACTIONS",
		"GITLAB_CI",
		"JENKINS_URL",
		"TRAVIS",
		"CIRCLECI",
	}

	for _, env := range ciEnvs {
		if os.Getenv(env) != "" {
			return true
		}
	}

	return false
}

// isInDevelopment checks for development environment indicators
func (d *RuntimeEnvDetector) isInDevelopment() bool {
	// Check for common development directories
	devDirs := []string{
		".git",
		".vscode",
		".idea",
		"node_modules",
	}

	for _, dir := range devDirs {
		if _, err := os.Stat(dir); err == nil {
			return true
		}
	}

	// Check for development environment variables
	devEnvs := []string{
		"DEVELOPMENT",
		"DEV",
		"DEBUG",
	}

	for _, env := range devEnvs {
		if os.Getenv(env) != "" {
			return true
		}
	}

	return false
}

// GetEnvironmentFromString converts string to Environment with validation
func GetEnvironmentFromString(envStr string) (Environment, error) {
	env := Environment(strings.ToLower(envStr))
	if !env.IsValid() {
		return "", fmt.Errorf("invalid environment: %s (valid: dev, test, prod)", envStr)
	}
	return env, nil
}

// GetDefaultEnvironment returns the default environment
func GetDefaultEnvironment() Environment {
	return EnvDevelopment
}