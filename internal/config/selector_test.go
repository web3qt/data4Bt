package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewDefaultConfigSelector(t *testing.T) {
	selector := NewDefaultConfigSelector()
	expectedPaths := []string{"./configs", "../configs", "."}
	
	if !reflect.DeepEqual(selector.searchPaths, expectedPaths) {
		t.Errorf("NewDefaultConfigSelector() searchPaths = %v, want %v", selector.searchPaths, expectedPaths)
	}
}

func TestNewConfigSelectorWithPaths(t *testing.T) {
	customPaths := []string{"/custom/path1", "/custom/path2"}
	selector := NewConfigSelectorWithPaths(customPaths)
	
	if !reflect.DeepEqual(selector.searchPaths, customPaths) {
		t.Errorf("NewConfigSelectorWithPaths() searchPaths = %v, want %v", selector.searchPaths, customPaths)
	}
}

func TestDefaultConfigSelector_SelectConfigFile(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create test config files
	testFiles := map[string]string{
		"config.yml":      "# default config",
		"config-dev.yml":  "# dev config",
		"config-test.yml": "# test config",
		"config-prod.yml": "# prod config",
	}

	for filename, content := range testFiles {
		err := os.WriteFile(filename, []byte(content), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", filename, err)
		}
	}

	selector := NewDefaultConfigSelector()

	tests := []struct {
		name       string
		configPath string
		env        Environment
		want       string
		wantErr    bool
	}{
		{
			name:       "explicit config path",
			configPath: "config.yml",
			env:        EnvDevelopment,
			want:       "config.yml",
			wantErr:    false,
		},
		{
			name:       "environment specific - dev",
			configPath: "",
			env:        EnvDevelopment,
			want:       "config-dev.yml",
			wantErr:    false,
		},
		{
			name:       "environment specific - test",
			configPath: "",
			env:        EnvTest,
			want:       "config-test.yml",
			wantErr:    false,
		},
		{
			name:       "environment specific - prod",
			configPath: "",
			env:        EnvProduction,
			want:       "config-prod.yml",
			wantErr:    false,
		},
		{
			name:       "fallback to default",
			configPath: "",
			env:        "",
			want:       "config.yml",
			wantErr:    false,
		},
		{
			name:       "invalid config path",
			configPath: "nonexistent.yml",
			env:        EnvDevelopment,
			want:       "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selector.SelectConfigFile(tt.configPath, tt.env)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultConfigSelector.SelectConfigFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DefaultConfigSelector.SelectConfigFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultConfigSelector_SelectConfigFile_NoFiles(t *testing.T) {
	// Create empty temporary directory
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	selector := NewDefaultConfigSelector()

	_, err := selector.SelectConfigFile("", EnvDevelopment)
	if err == nil {
		t.Error("Expected error when no config files exist, but got nil")
	}
}

func TestDefaultConfigSelector_findEnvironmentConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create test file
	err := os.WriteFile("config-prod.yml", []byte("# prod config"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	selector := NewDefaultConfigSelector()

	tests := []struct {
		name string
		env  Environment
		want string
	}{
		{"existing env config", EnvProduction, "config-prod.yml"},
		{"non-existing env config", EnvDevelopment, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selector.findEnvironmentConfigFile(tt.env)
			if err != nil {
				t.Errorf("DefaultConfigSelector.findEnvironmentConfigFile() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("DefaultConfigSelector.findEnvironmentConfigFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultConfigSelector_findDefaultConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	tests := []struct {
		name      string
		setupFile string
		want      string
	}{
		{"config.yml exists", "config.yml", "config.yml"},
		{"config.yaml exists", "config.yaml", "config.yaml"},
		{"no default config", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing files
			os.Remove("config.yml")
			os.Remove("config.yaml")

			if tt.setupFile != "" {
				err := os.WriteFile(tt.setupFile, []byte("# default config"), 0644)
				if err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
			}

			selector := NewDefaultConfigSelector()
			got, err := selector.findDefaultConfigFile()
			if err != nil {
				t.Errorf("DefaultConfigSelector.findDefaultConfigFile() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("DefaultConfigSelector.findDefaultConfigFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultConfigSelector_validateConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	selector := NewDefaultConfigSelector()

	tests := []struct {
		name     string
		setup    func() string
		wantErr  bool
	}{
		{
			name: "valid yml file",
			setup: func() string {
				filename := "valid.yml"
				os.WriteFile(filename, []byte("key: value"), 0644)
				return filename
			},
			wantErr: false,
		},
		{
			name: "valid yaml file",
			setup: func() string {
				filename := "valid.yaml"
				os.WriteFile(filename, []byte("key: value"), 0644)
				return filename
			},
			wantErr: false,
		},
		{
			name: "nonexistent file",
			setup: func() string {
				return "nonexistent.yml"
			},
			wantErr: true,
		},
		{
			name: "directory instead of file",
			setup: func() string {
				dirname := "testdir.yml"
				os.Mkdir(dirname, 0755)
				return dirname
			},
			wantErr: true,
		},
		{
			name: "invalid extension",
			setup: func() string {
				filename := "invalid.txt"
				os.WriteFile(filename, []byte("content"), 0644)
				return filename
			},
			wantErr: true,
		},
		{
			name: "empty file",
			setup: func() string {
				filename := "empty.yml"
				os.WriteFile(filename, []byte(""), 0644)
				return filename
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filepath := tt.setup()
			err := selector.validateConfigFile(filepath)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultConfigSelector.validateConfigFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigSelector_GetSearchPaths(t *testing.T) {
	originalPaths := []string{"/path1", "/path2"}
	selector := NewConfigSelectorWithPaths(originalPaths)
	
	got := selector.GetSearchPaths()
	
	// Check that we get a copy, not the original slice
	if &got[0] == &selector.searchPaths[0] {
		t.Error("GetSearchPaths() should return a copy, not the original slice")
	}
	
	if !reflect.DeepEqual(got, originalPaths) {
		t.Errorf("GetSearchPaths() = %v, want %v", got, originalPaths)
	}
}

func TestDefaultConfigSelector_AddSearchPath(t *testing.T) {
	selector := NewDefaultConfigSelector()
	originalLen := len(selector.searchPaths)
	
	selector.AddSearchPath("/new/path")
	
	if len(selector.searchPaths) != originalLen+1 {
		t.Errorf("AddSearchPath() should increase search paths length by 1")
	}
	
	if selector.searchPaths[0] != "/new/path" {
		t.Errorf("AddSearchPath() should add path to the beginning, got %v", selector.searchPaths[0])
	}
}

func TestDefaultConfigSelector_SetSearchPaths(t *testing.T) {
	selector := NewDefaultConfigSelector()
	newPaths := []string{"/custom1", "/custom2"}
	
	selector.SetSearchPaths(newPaths)
	
	if !reflect.DeepEqual(selector.searchPaths, newPaths) {
		t.Errorf("SetSearchPaths() = %v, want %v", selector.searchPaths, newPaths)
	}
	
	// Verify it's a copy
	newPaths[0] = "/modified"
	if selector.searchPaths[0] == "/modified" {
		t.Error("SetSearchPaths() should make a copy of the input slice")
	}
}

func TestDefaultConfigSelector_ListAvailableConfigs(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create test config files
	testFiles := []string{
		"config.yml",
		"config-dev.yml",
		"config-prod.yml",
	}

	for _, filename := range testFiles {
		err := os.WriteFile(filename, []byte("# test config"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", filename, err)
		}
	}

	selector := NewDefaultConfigSelector()
	configs, err := selector.ListAvailableConfigs()
	if err != nil {
		t.Fatalf("ListAvailableConfigs() error = %v", err)
	}

	if len(configs) != 3 {
		t.Errorf("ListAvailableConfigs() found %d configs, want 3", len(configs))
	}

	// Check that we found the expected configs
	foundFiles := make(map[string]bool)
	for _, config := range configs {
		foundFiles[filepath.Base(config.Path)] = true
		if config.Size <= 0 {
			t.Errorf("Config file %s should have size > 0, got %d", config.Path, config.Size)
		}
	}

	for _, expectedFile := range testFiles {
		if !foundFiles[expectedFile] {
			t.Errorf("Expected to find config file %s, but it was not listed", expectedFile)
		}
	}
}

func TestDefaultConfigSelector_HasConfigForEnvironment(t *testing.T) {
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create only dev config
	err := os.WriteFile("config-dev.yml", []byte("# dev config"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	selector := NewDefaultConfigSelector()

	tests := []struct {
		name string
		env  Environment
		want bool
	}{
		{"existing config", EnvDevelopment, true},
		{"non-existing config", EnvProduction, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selector.HasConfigForEnvironment(tt.env)
			if got != tt.want {
				t.Errorf("DefaultConfigSelector.HasConfigForEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetConfigFileType(t *testing.T) {
	tests := []struct {
		name        string
		configPath  string
		wantEnv     Environment
		wantDefault bool
		wantErr     bool
	}{
		{"dev config", "config-dev.yml", EnvDevelopment, false, false},
		{"test config", "config-test.yml", EnvTest, false, false},
		{"prod config", "config-prod.yml", EnvProduction, false, false},
		{"dev config yaml", "config-dev.yaml", EnvDevelopment, false, false},
		{"default config yml", "config.yml", "", true, false},
		{"default config yaml", "config.yaml", "", true, false},
		{"path with dir", "/path/to/config-prod.yml", EnvProduction, false, false},
		{"empty path", "", "", false, true},
		{"invalid pattern", "myconfig.yml", "", false, true},
		{"invalid env", "config-invalid.yml", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEnv, gotDefault, err := GetConfigFileType(tt.configPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfigFileType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotEnv != tt.wantEnv {
				t.Errorf("GetConfigFileType() gotEnv = %v, want %v", gotEnv, tt.wantEnv)
			}
			if gotDefault != tt.wantDefault {
				t.Errorf("GetConfigFileType() gotDefault = %v, want %v", gotDefault, tt.wantDefault)
			}
		})
	}
}

// Benchmark tests
func BenchmarkDefaultConfigSelector_SelectConfigFile(b *testing.B) {
	tempDir := b.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create test files
	os.WriteFile("config.yml", []byte("# config"), 0644)
	os.WriteFile("config-dev.yml", []byte("# dev config"), 0644)

	selector := NewDefaultConfigSelector()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = selector.SelectConfigFile("", EnvDevelopment)
	}
}

func BenchmarkDefaultConfigSelector_ListAvailableConfigs(b *testing.B) {
	tempDir := b.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// Create test files
	testFiles := []string{"config.yml", "config-dev.yml", "config-test.yml", "config-prod.yml"}
	for _, filename := range testFiles {
		os.WriteFile(filename, []byte("# config"), 0644)
	}

	selector := NewDefaultConfigSelector()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = selector.ListAvailableConfigs()
	}
}