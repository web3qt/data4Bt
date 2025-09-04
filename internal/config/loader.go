package config

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/viper"
)

// EnhancedConfigLoader provides enhanced configuration loading with environment detection
type EnhancedConfigLoader struct {
	envDetector    *EnvironmentDetectorRegistry
	configSelector ConfigSelector
	viper         *viper.Viper
	debugMode     bool
}

// NewEnhancedConfigLoader creates a new enhanced config loader
func NewEnhancedConfigLoader() *EnhancedConfigLoader {
	return &EnhancedConfigLoader{
		envDetector:    NewEnvironmentDetectorRegistry(),
		configSelector: NewDefaultConfigSelector(),
		viper:         viper.New(),
		debugMode:     os.Getenv("BDL_DEBUG") != "",
	}
}

// LoadConfig loads configuration with environment detection and file selection
func (l *EnhancedConfigLoader) LoadConfig(configPath string) (*Config, error) {
	// Step 1: Detect environment
	env := l.detectEnvironment(configPath)
	if l.debugMode {
		log.Printf("[DEBUG] Detected environment: %s", env)
	}

	// Step 2: Select configuration file
	selectedConfigFile, err := l.selectConfigFile(configPath, env)
	if err != nil {
		return nil, fmt.Errorf("failed to select config file: %w", err)
	}
	if l.debugMode {
		log.Printf("[DEBUG] Selected config file: %s", selectedConfigFile)
	}

	// Step 3: Load and merge configuration
	config, err := l.loadAndMergeConfig(selectedConfigFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Step 4: Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	if l.debugMode {
		log.Printf("[DEBUG] Configuration loaded successfully from: %s", selectedConfigFile)
	}

	return config, nil
}

// detectEnvironment detects the current environment
func (l *EnhancedConfigLoader) detectEnvironment(configPath string) Environment {
	// If a specific config file is provided, try to determine environment from filename
	if configPath != "" {
		if env, isDefault, err := GetConfigFileType(configPath); err == nil && !isDefault {
			if l.debugMode {
				log.Printf("[DEBUG] Environment detected from config file: %s", env)
			}
			return env
		}
	}

	// Use environment detector registry
	detectedEnv := l.envDetector.DetectEnvironment()
	if l.debugMode {
		log.Printf("[DEBUG] Environment detected by registry: %s", detectedEnv)
	}

	return detectedEnv
}

// selectConfigFile selects the appropriate configuration file
func (l *EnhancedConfigLoader) selectConfigFile(configPath string, env Environment) (string, error) {
	selectedFile, err := l.configSelector.SelectConfigFile(configPath, env)
	if err != nil {
		return "", err
	}

	// Verify the selected file exists and is readable
	if selectedFile != "" {
		if _, err := os.Stat(selectedFile); err != nil {
			return "", fmt.Errorf("selected config file '%s' is not accessible: %w", selectedFile, err)
		}
	}

	return selectedFile, nil
}

// loadAndMergeConfig loads configuration from file and applies environment variable overrides
func (l *EnhancedConfigLoader) loadAndMergeConfig(configFile string) (*Config, error) {
	// Reset viper instance
	l.viper = viper.New()

	// Set environment variable configuration
	l.viper.SetEnvPrefix("BDL")
	l.viper.AutomaticEnv()

	// Set default values
	setDefaults(l.viper)

	// Load configuration file if specified
	if configFile != "" {
		l.viper.SetConfigFile(configFile)
		if err := l.viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file '%s': %w", configFile, err)
		}
		if l.debugMode {
			log.Printf("[DEBUG] Loaded config from file: %s", configFile)
		}
	} else {
		if l.debugMode {
			log.Printf("[DEBUG] Using default configuration values only")
		}
	}

	// Parse configuration into struct
	var config Config
	if err := l.viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// GetConfigInfo returns information about the current configuration setup
func (l *EnhancedConfigLoader) GetConfigInfo() (*ConfigInfo, error) {
	env := l.envDetector.DetectEnvironment()
	
	// Type assert to DefaultConfigSelector to access specific methods
	defaultSelector, ok := l.configSelector.(*DefaultConfigSelector)
	if !ok {
		return nil, fmt.Errorf("config selector is not a DefaultConfigSelector")
	}
	
	availableConfigs, err := defaultSelector.ListAvailableConfigs()
	if err != nil {
		return nil, err
	}

	return &ConfigInfo{
		DetectedEnvironment: env,
		AvailableConfigs:    availableConfigs,
		SearchPaths:         defaultSelector.GetSearchPaths(),
	}, nil
}

// ConfigInfo contains information about the configuration setup
type ConfigInfo struct {
	DetectedEnvironment Environment       `json:"detected_environment"`
	AvailableConfigs    []ConfigFileInfo  `json:"available_configs"`
	SearchPaths         []string          `json:"search_paths"`
}

// SetDebugMode enables or disables debug logging
func (l *EnhancedConfigLoader) SetDebugMode(enabled bool) {
	l.debugMode = enabled
}

// AddSearchPath adds a search path to the config selector
func (l *EnhancedConfigLoader) AddSearchPath(path string) {
	if defaultSelector, ok := l.configSelector.(*DefaultConfigSelector); ok {
		defaultSelector.AddSearchPath(path)
	}
}

// LoadWithEnv loads configuration for a specific environment
func LoadWithEnv(configPath string, env Environment) (*Config, error) {
	loader := NewEnhancedConfigLoader()
	
	// Override environment detection if env is specified
	if env != "" && env.IsValid() {
		if configPath == "" {
			// Find config file for the specified environment
			selectedFile, err := loader.configSelector.SelectConfigFile("", env)
			if err != nil {
				return nil, fmt.Errorf("failed to find config for environment %s: %w", env, err)
			}
			configPath = selectedFile
		}
	}
	
	return loader.LoadConfig(configPath)
}

// Enhanced Load function that maintains backward compatibility
func LoadEnhanced(configPath string) (*Config, error) {
	loader := NewEnhancedConfigLoader()
	return loader.LoadConfig(configPath)
}

// GetAvailableConfigs returns information about available configuration files
func GetAvailableConfigs() ([]ConfigFileInfo, error) {
	selector := NewDefaultConfigSelector()
	return selector.ListAvailableConfigs()
}

// DetectCurrentEnvironment detects the current environment without loading config
func DetectCurrentEnvironment() Environment {
	detector := NewEnvironmentDetectorRegistry()
	return detector.DetectEnvironment()
}

// ValidateConfigFile validates a configuration file without loading it fully
func ValidateConfigFile(configPath string) error {
	if configPath == "" {
		return fmt.Errorf("config path is empty")
	}

	selector := NewDefaultConfigSelector()
	return selector.validateConfigFile(configPath)
}

// GetConfigFileForEnvironment returns the config file path for a specific environment
func GetConfigFileForEnvironment(env Environment) (string, error) {
	selector := NewDefaultConfigSelector()
	return selector.SelectConfigFile("", env)
}