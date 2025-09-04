package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ConfigSelector defines the interface for configuration file selection
type ConfigSelector interface {
	SelectConfigFile(configPath string, env Environment) (string, error)
}

// DefaultConfigSelector implements ConfigSelector with default search behavior
type DefaultConfigSelector struct {
	searchPaths []string
}

// NewDefaultConfigSelector creates a new DefaultConfigSelector with standard search paths
func NewDefaultConfigSelector() *DefaultConfigSelector {
	return &DefaultConfigSelector{
		searchPaths: []string{
			"./configs",
			"../configs",
			".",
		},
	}
}

// NewConfigSelectorWithPaths creates a new DefaultConfigSelector with custom search paths
func NewConfigSelectorWithPaths(paths []string) *DefaultConfigSelector {
	return &DefaultConfigSelector{
		searchPaths: paths,
	}
}

// SelectConfigFile selects the appropriate configuration file based on priority rules
func (s *DefaultConfigSelector) SelectConfigFile(configPath string, env Environment) (string, error) {
	// Priority 1: If configPath is explicitly specified, use it directly
	if configPath != "" {
		if err := s.validateConfigFile(configPath); err != nil {
			return "", fmt.Errorf("specified config file '%s' is invalid: %w", configPath, err)
		}
		return configPath, nil
	}

	// Priority 2: Try environment-specific config file
	if env != "" && env.IsValid() {
		envConfigFile, err := s.findEnvironmentConfigFile(env)
		if err != nil {
			return "", err
		}
		if envConfigFile != "" {
			return envConfigFile, nil
		}
	}

	// Priority 3: Try default config file
	defaultConfigFile, err := s.findDefaultConfigFile()
	if err != nil {
		return "", err
	}
	if defaultConfigFile != "" {
		return defaultConfigFile, nil
	}

	// Priority 4: Return error if no config file found
	return "", fmt.Errorf("no configuration file found in search paths: %v", s.searchPaths)
}

// findEnvironmentConfigFile searches for environment-specific config files
func (s *DefaultConfigSelector) findEnvironmentConfigFile(env Environment) (string, error) {
	filename := fmt.Sprintf("config-%s.yml", env.String())
	
	for _, searchPath := range s.searchPaths {
		configFile := filepath.Join(searchPath, filename)
		if s.fileExists(configFile) {
			if err := s.validateConfigFile(configFile); err != nil {
				continue // Skip invalid files and try next path
			}
			return configFile, nil
		}
	}
	
	return "", nil // Not found, but not an error
}

// findDefaultConfigFile searches for the default config file
func (s *DefaultConfigSelector) findDefaultConfigFile() (string, error) {
	defaultFilenames := []string{"config.yml", "config.yaml"}
	
	for _, searchPath := range s.searchPaths {
		for _, filename := range defaultFilenames {
			configFile := filepath.Join(searchPath, filename)
			if s.fileExists(configFile) {
				if err := s.validateConfigFile(configFile); err != nil {
					continue // Skip invalid files and try next
				}
				return configFile, nil
			}
		}
	}
	
	return "", nil // Not found, but not an error
}

// fileExists checks if a file exists and is readable
func (s *DefaultConfigSelector) fileExists(filepath string) bool {
	info, err := os.Stat(filepath)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// validateConfigFile performs basic validation on a config file
func (s *DefaultConfigSelector) validateConfigFile(filepath string) error {
	// Check if file exists and is readable
	info, err := os.Stat(filepath)
	if err != nil {
		return fmt.Errorf("cannot access file: %w", err)
	}
	
	// Check if it's a regular file
	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file")
	}
	
	// Check file extension
	ext := strings.ToLower(filepath[strings.LastIndex(filepath, "."):])
	if ext != ".yml" && ext != ".yaml" {
		return fmt.Errorf("unsupported file extension '%s', expected .yml or .yaml", ext)
	}
	
	// Check file size (basic sanity check)
	if info.Size() == 0 {
		return fmt.Errorf("config file is empty")
	}
	
	if info.Size() > 10*1024*1024 { // 10MB limit
		return fmt.Errorf("config file is too large (%d bytes), maximum allowed is 10MB", info.Size())
	}
	
	return nil
}

// GetSearchPaths returns the current search paths
func (s *DefaultConfigSelector) GetSearchPaths() []string {
	return append([]string{}, s.searchPaths...) // Return a copy
}

// AddSearchPath adds a new search path to the beginning of the search list
func (s *DefaultConfigSelector) AddSearchPath(path string) {
	s.searchPaths = append([]string{path}, s.searchPaths...)
}

// SetSearchPaths replaces the current search paths with new ones
func (s *DefaultConfigSelector) SetSearchPaths(paths []string) {
	s.searchPaths = append([]string{}, paths...) // Make a copy
}

// ConfigFileInfo contains information about a found configuration file
type ConfigFileInfo struct {
	Path        string
	Environment Environment
	IsDefault   bool
	Size        int64
}

// ListAvailableConfigs returns information about all available configuration files
func (s *DefaultConfigSelector) ListAvailableConfigs() ([]ConfigFileInfo, error) {
	var configs []ConfigFileInfo
	seenFiles := make(map[string]bool)
	
	// Search for environment-specific configs
	envs := []Environment{EnvDevelopment, EnvTest, EnvProduction}
	for _, env := range envs {
		filename := fmt.Sprintf("config-%s.yml", env.String())
		for _, searchPath := range s.searchPaths {
			configFile := filepath.Join(searchPath, filename)
			if s.fileExists(configFile) && !seenFiles[configFile] {
				if err := s.validateConfigFile(configFile); err == nil {
					info, _ := os.Stat(configFile)
					configs = append(configs, ConfigFileInfo{
						Path:        configFile,
						Environment: env,
						IsDefault:   false,
						Size:        info.Size(),
					})
					seenFiles[configFile] = true
				}
			}
		}
	}
	
	// Search for default configs
	defaultFilenames := []string{"config.yml", "config.yaml"}
	for _, searchPath := range s.searchPaths {
		for _, filename := range defaultFilenames {
			configFile := filepath.Join(searchPath, filename)
			if s.fileExists(configFile) && !seenFiles[configFile] {
				if err := s.validateConfigFile(configFile); err == nil {
					info, _ := os.Stat(configFile)
					configs = append(configs, ConfigFileInfo{
						Path:        configFile,
						Environment: "", // Default config doesn't have specific environment
						IsDefault:   true,
						Size:        info.Size(),
					})
					seenFiles[configFile] = true
				}
			}
		}
	}
	
	return configs, nil
}

// FindConfigForEnvironment is a convenience method to find config file for a specific environment
func (s *DefaultConfigSelector) FindConfigForEnvironment(env Environment) (string, error) {
	return s.SelectConfigFile("", env)
}

// HasConfigForEnvironment checks if a config file exists for the specified environment
func (s *DefaultConfigSelector) HasConfigForEnvironment(env Environment) bool {
	configFile, err := s.findEnvironmentConfigFile(env)
	return err == nil && configFile != ""
}

// GetConfigFileType determines the type of a config file based on its name
func GetConfigFileType(configPath string) (Environment, bool, error) {
	if configPath == "" {
		return "", false, fmt.Errorf("empty config path")
	}
	
	filename := filepath.Base(configPath)
	
	// Check for environment-specific config
	if strings.HasPrefix(filename, "config-") && (strings.HasSuffix(filename, ".yml") || strings.HasSuffix(filename, ".yaml")) {
		// Extract environment from filename
		envPart := filename[7:] // Remove "config-" prefix
		if strings.HasSuffix(envPart, ".yml") {
			envPart = envPart[:len(envPart)-4] // Remove ".yml" suffix
		} else if strings.HasSuffix(envPart, ".yaml") {
			envPart = envPart[:len(envPart)-5] // Remove ".yaml" suffix
		}
		
		env := Environment(envPart)
		if env.IsValid() {
			return env, false, nil // Environment-specific, not default
		}
	}
	
	// Check for default config
	if filename == "config.yml" || filename == "config.yaml" {
		return "", true, nil // Default config
	}
	
	return "", false, fmt.Errorf("unrecognized config file pattern: %s", filename)
}