package config

import (
	"os"
	"testing"
)

func TestEnvironment_String(t *testing.T) {
	tests := []struct {
		name string
		env  Environment
		want string
	}{
		{"development", EnvDevelopment, "dev"},
		{"test", EnvTest, "test"},
		{"production", EnvProduction, "prod"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.env.String(); got != tt.want {
				t.Errorf("Environment.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnvironment_IsValid(t *testing.T) {
	tests := []struct {
		name string
		env  Environment
		want bool
	}{
		{"valid dev", EnvDevelopment, true},
		{"valid test", EnvTest, true},
		{"valid prod", EnvProduction, true},
		{"invalid empty", Environment(""), false},
		{"invalid random", Environment("invalid"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.env.IsValid(); got != tt.want {
				t.Errorf("Environment.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommandLineEnvDetector_Detect(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantEnv    Environment
		wantConf   int
		wantErr    bool
	}{
		{
			name:     "no env flag",
			args:     []string{"program"},
			wantEnv:  "",
			wantConf: 0,
			wantErr:  false,
		},
		{
			name:     "env flag with equals",
			args:     []string{"program", "-env=prod"},
			wantEnv:  EnvProduction,
			wantConf: 100,
			wantErr:  false,
		},
		{
			name:     "env flag separate",
			args:     []string{"program", "-env", "test"},
			wantEnv:  EnvTest,
			wantConf: 100,
			wantErr:  false,
		},
		{
			name:     "invalid env",
			args:     []string{"program", "-env=invalid"},
			wantEnv:  "",
			wantConf: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original args
			originalArgs := os.Args
			defer func() { os.Args = originalArgs }()

			// Set test args
			os.Args = tt.args

			detector := &CommandLineEnvDetector{}
			gotEnv, gotConf, err := detector.Detect()

			if (err != nil) != tt.wantErr {
				t.Errorf("CommandLineEnvDetector.Detect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotEnv != tt.wantEnv {
				t.Errorf("CommandLineEnvDetector.Detect() gotEnv = %v, want %v", gotEnv, tt.wantEnv)
			}
			if gotConf != tt.wantConf {
				t.Errorf("CommandLineEnvDetector.Detect() gotConf = %v, want %v", gotConf, tt.wantConf)
			}
		})
	}
}

func TestEnvVarDetector_Detect(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		wantEnv  Environment
		wantConf int
		wantErr  bool
	}{
		{
			name:     "no env var",
			envVar:   "",
			wantEnv:  "",
			wantConf: 0,
			wantErr:  false,
		},
		{
			name:     "valid prod env",
			envVar:   "prod",
			wantEnv:  EnvProduction,
			wantConf: 90,
			wantErr:  false,
		},
		{
			name:     "valid dev env",
			envVar:   "dev",
			wantEnv:  EnvDevelopment,
			wantConf: 90,
			wantErr:  false,
		},
		{
			name:     "invalid env",
			envVar:   "invalid",
			wantEnv:  "",
			wantConf: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original env var
			originalEnv := os.Getenv("BDL_ENV")
			defer func() {
				if originalEnv != "" {
					os.Setenv("BDL_ENV", originalEnv)
				} else {
					os.Unsetenv("BDL_ENV")
				}
			}()

			// Set test env var
			if tt.envVar != "" {
				os.Setenv("BDL_ENV", tt.envVar)
			} else {
				os.Unsetenv("BDL_ENV")
			}

			detector := &EnvVarDetector{}
			gotEnv, gotConf, err := detector.Detect()

			if (err != nil) != tt.wantErr {
				t.Errorf("EnvVarDetector.Detect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotEnv != tt.wantEnv {
				t.Errorf("EnvVarDetector.Detect() gotEnv = %v, want %v", gotEnv, tt.wantEnv)
			}
			if gotConf != tt.wantConf {
				t.Errorf("EnvVarDetector.Detect() gotConf = %v, want %v", gotConf, tt.wantConf)
			}
		})
	}
}

func TestFileExistenceDetector_Detect(t *testing.T) {
	// Create temporary directory for testing
	tempDir := t.TempDir()
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	tests := []struct {
		name       string
		setupFiles []string
		wantEnv    Environment
		wantConf   int
	}{
		{
			name:       "no config files",
			setupFiles: []string{},
			wantEnv:    "",
			wantConf:   0,
		},
		{
			name:       "prod config exists",
			setupFiles: []string{"config-prod.yml"},
			wantEnv:    EnvProduction,
			wantConf:   70,
		},
		{
			name:       "test config exists",
			setupFiles: []string{"config-test.yml"},
			wantEnv:    EnvTest,
			wantConf:   70,
		},
		{
			name:       "multiple configs - prod preferred",
			setupFiles: []string{"config-dev.yml", "config-test.yml", "config-prod.yml"},
			wantEnv:    EnvProduction,
			wantConf:   70,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test files
			for _, file := range tt.setupFiles {
				f, err := os.Create(file)
				if err != nil {
					t.Fatalf("Failed to create test file %s: %v", file, err)
				}
				f.Close()
				defer os.Remove(file)
			}

			detector := &FileExistenceDetector{}
			gotEnv, gotConf, err := detector.Detect()

			if err != nil {
				t.Errorf("FileExistenceDetector.Detect() error = %v", err)
				return
			}
			if gotEnv != tt.wantEnv {
				t.Errorf("FileExistenceDetector.Detect() gotEnv = %v, want %v", gotEnv, tt.wantEnv)
			}
			if gotConf != tt.wantConf {
				t.Errorf("FileExistenceDetector.Detect() gotConf = %v, want %v", gotConf, tt.wantConf)
			}
		})
	}
}

func TestRuntimeEnvDetector_isInContainer(t *testing.T) {
	detector := &RuntimeEnvDetector{}

	// Test with container environment variable
	originalEnv := os.Getenv("DOCKER_CONTAINER")
	defer func() {
		if originalEnv != "" {
			os.Setenv("DOCKER_CONTAINER", originalEnv)
		} else {
			os.Unsetenv("DOCKER_CONTAINER")
		}
	}()

	os.Setenv("DOCKER_CONTAINER", "true")
	if !detector.isInContainer() {
		t.Error("Expected isInContainer() to return true when DOCKER_CONTAINER is set")
	}

	os.Unsetenv("DOCKER_CONTAINER")
	// Note: We can't easily test file-based detection in unit tests
	// as it depends on the actual filesystem
}

func TestRuntimeEnvDetector_isInCI(t *testing.T) {
	detector := &RuntimeEnvDetector{}

	// Test with CI environment variable
	originalEnv := os.Getenv("CI")
	defer func() {
		if originalEnv != "" {
			os.Setenv("CI", originalEnv)
		} else {
			os.Unsetenv("CI")
		}
	}()

	os.Setenv("CI", "true")
	if !detector.isInCI() {
		t.Error("Expected isInCI() to return true when CI is set")
	}

	os.Unsetenv("CI")
	if detector.isInCI() {
		t.Error("Expected isInCI() to return false when no CI env vars are set")
	}
}

func TestEnvironmentDetectorRegistry_DetectEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		cleanup func()
		want    Environment
	}{
		{
			name: "command line takes precedence",
			setup: func() {
				os.Args = []string{"program", "-env=prod"}
				os.Setenv("BDL_ENV", "dev")
			},
			cleanup: func() {
				os.Args = []string{"program"}
				os.Unsetenv("BDL_ENV")
			},
			want: EnvProduction,
		},
		{
			name: "env var when no command line",
			setup: func() {
				os.Args = []string{"program"}
				os.Setenv("BDL_ENV", "test")
			},
			cleanup: func() {
				os.Unsetenv("BDL_ENV")
			},
			want: EnvTest,
		},
		{
			name: "default when nothing detected",
			setup: func() {
				os.Args = []string{"program"}
				os.Unsetenv("BDL_ENV")
			},
			cleanup: func() {},
			want: EnvDevelopment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original state
			originalArgs := os.Args
			originalEnv := os.Getenv("BDL_ENV")
			defer func() {
				os.Args = originalArgs
				if originalEnv != "" {
					os.Setenv("BDL_ENV", originalEnv)
				} else {
					os.Unsetenv("BDL_ENV")
				}
			}()

			tt.setup()
			defer tt.cleanup()

			registry := NewEnvironmentDetectorRegistry()
			got := registry.DetectEnvironment()

			if got != tt.want {
				t.Errorf("EnvironmentDetectorRegistry.DetectEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnvironmentFromString(t *testing.T) {
	tests := []struct {
		name    string
		envStr  string
		want    Environment
		wantErr bool
	}{
		{"valid dev", "dev", EnvDevelopment, false},
		{"valid test", "test", EnvTest, false},
		{"valid prod", "prod", EnvProduction, false},
		{"valid uppercase", "DEV", EnvDevelopment, false},
		{"valid mixed case", "Test", EnvTest, false},
		{"invalid", "invalid", "", true},
		{"empty", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetEnvironmentFromString(tt.envStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEnvironmentFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetEnvironmentFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDefaultEnvironment(t *testing.T) {
	got := GetDefaultEnvironment()
	want := EnvDevelopment
	if got != want {
		t.Errorf("GetDefaultEnvironment() = %v, want %v", got, want)
	}
}

// Benchmark tests
func BenchmarkEnvironmentDetectorRegistry_DetectEnvironment(b *testing.B) {
	registry := NewEnvironmentDetectorRegistry()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = registry.DetectEnvironment()
	}
}

func BenchmarkCommandLineEnvDetector_Detect(b *testing.B) {
	detector := &CommandLineEnvDetector{}
	os.Args = []string{"program", "-env=prod"}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = detector.Detect()
	}
}