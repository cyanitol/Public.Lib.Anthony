package security

import (
	"math"
	"testing"
)

func TestSafeCastUint32ToUint16(t *testing.T) {
	tests := []struct {
		name      string
		input     uint32
		want      uint16
		wantError bool
	}{
		{
			name:      "zero",
			input:     0,
			want:      0,
			wantError: false,
		},
		{
			name:      "small value",
			input:     100,
			want:      100,
			wantError: false,
		},
		{
			name:      "max uint16",
			input:     math.MaxUint16,
			want:      math.MaxUint16,
			wantError: false,
		},
		{
			name:      "max uint16 + 1",
			input:     math.MaxUint16 + 1,
			want:      0,
			wantError: true,
		},
		{
			name:      "max uint32",
			input:     math.MaxUint32,
			want:      0,
			wantError: true,
		},
		{
			name:      "boundary - 1",
			input:     math.MaxUint16 - 1,
			want:      math.MaxUint16 - 1,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeCastUint32ToUint16(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeCastUint32ToUint16() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeCastUint32ToUint16() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerOverflow {
				t.Errorf("SafeCastUint32ToUint16() error = %v, want %v", err, ErrIntegerOverflow)
			}
		})
	}
}

func TestSafeCastInt64ToInt32(t *testing.T) {
	tests := []struct {
		name      string
		input     int64
		want      int32
		wantError bool
	}{
		{
			name:      "zero",
			input:     0,
			want:      0,
			wantError: false,
		},
		{
			name:      "positive value",
			input:     12345,
			want:      12345,
			wantError: false,
		},
		{
			name:      "negative value",
			input:     -12345,
			want:      -12345,
			wantError: false,
		},
		{
			name:      "max int32",
			input:     math.MaxInt32,
			want:      math.MaxInt32,
			wantError: false,
		},
		{
			name:      "min int32",
			input:     math.MinInt32,
			want:      math.MinInt32,
			wantError: false,
		},
		{
			name:      "max int32 + 1",
			input:     math.MaxInt32 + 1,
			want:      0,
			wantError: true,
		},
		{
			name:      "min int32 - 1",
			input:     math.MinInt32 - 1,
			want:      0,
			wantError: true,
		},
		{
			name:      "max int64",
			input:     math.MaxInt64,
			want:      0,
			wantError: true,
		},
		{
			name:      "min int64",
			input:     math.MinInt64,
			want:      0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeCastInt64ToInt32(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeCastInt64ToInt32() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeCastInt64ToInt32() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerOverflow {
				t.Errorf("SafeCastInt64ToInt32() error = %v, want %v", err, ErrIntegerOverflow)
			}
		})
	}
}

func TestSafeAddUint32(t *testing.T) {
	tests := []struct {
		name      string
		a         uint32
		b         uint32
		want      uint32
		wantError bool
	}{
		{
			name:      "zero + zero",
			a:         0,
			b:         0,
			want:      0,
			wantError: false,
		},
		{
			name:      "small values",
			a:         100,
			b:         200,
			want:      300,
			wantError: false,
		},
		{
			name:      "max - 1 + 1",
			a:         math.MaxUint32 - 1,
			b:         1,
			want:      math.MaxUint32,
			wantError: false,
		},
		{
			name:      "max + 1 (overflow)",
			a:         math.MaxUint32,
			b:         1,
			want:      0,
			wantError: true,
		},
		{
			name:      "large + large (overflow)",
			a:         math.MaxUint32 / 2 + 1,
			b:         math.MaxUint32 / 2 + 1,
			want:      0,
			wantError: true,
		},
		{
			name:      "max + max (overflow)",
			a:         math.MaxUint32,
			b:         math.MaxUint32,
			want:      0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeAddUint32(tt.a, tt.b)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeAddUint32() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeAddUint32() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerOverflow {
				t.Errorf("SafeAddUint32() error = %v, want %v", err, ErrIntegerOverflow)
			}
		})
	}
}

func TestSafeSubUint32(t *testing.T) {
	tests := []struct {
		name      string
		a         uint32
		b         uint32
		want      uint32
		wantError bool
	}{
		{
			name:      "zero - zero",
			a:         0,
			b:         0,
			want:      0,
			wantError: false,
		},
		{
			name:      "large - small",
			a:         300,
			b:         100,
			want:      200,
			wantError: false,
		},
		{
			name:      "equal values",
			a:         100,
			b:         100,
			want:      0,
			wantError: false,
		},
		{
			name:      "small - large (underflow)",
			a:         100,
			b:         200,
			want:      0,
			wantError: true,
		},
		{
			name:      "zero - one (underflow)",
			a:         0,
			b:         1,
			want:      0,
			wantError: true,
		},
		{
			name:      "max - max",
			a:         math.MaxUint32,
			b:         math.MaxUint32,
			want:      0,
			wantError: false,
		},
		{
			name:      "max - 0",
			a:         math.MaxUint32,
			b:         0,
			want:      math.MaxUint32,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeSubUint32(tt.a, tt.b)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeSubUint32() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeSubUint32() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerUnderflow {
				t.Errorf("SafeSubUint32() error = %v, want %v", err, ErrIntegerUnderflow)
			}
		})
	}
}

func TestSafeCastIntToUint16(t *testing.T) {
	tests := []struct {
		name      string
		input     int
		want      uint16
		wantError bool
	}{
		{
			name:      "zero",
			input:     0,
			want:      0,
			wantError: false,
		},
		{
			name:      "positive value",
			input:     1000,
			want:      1000,
			wantError: false,
		},
		{
			name:      "max uint16",
			input:     math.MaxUint16,
			want:      math.MaxUint16,
			wantError: false,
		},
		{
			name:      "negative value",
			input:     -1,
			want:      0,
			wantError: true,
		},
		{
			name:      "max uint16 + 1",
			input:     math.MaxUint16 + 1,
			want:      0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeCastIntToUint16(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeCastIntToUint16() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeCastIntToUint16() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerOverflow {
				t.Errorf("SafeCastIntToUint16() error = %v, want %v", err, ErrIntegerOverflow)
			}
		})
	}
}

func TestSafeCastIntToUint32(t *testing.T) {
	tests := []struct {
		name      string
		input     int
		want      uint32
		wantError bool
	}{
		{
			name:      "zero",
			input:     0,
			want:      0,
			wantError: false,
		},
		{
			name:      "positive value",
			input:     1000000,
			want:      1000000,
			wantError: false,
		},
		{
			name:      "negative value",
			input:     -1,
			want:      0,
			wantError: true,
		},
		{
			name:      "large negative",
			input:     -1000000,
			want:      0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeCastIntToUint32(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("SafeCastIntToUint32() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("SafeCastIntToUint32() = %v, want %v", got, tt.want)
			}
			if tt.wantError && err != ErrIntegerOverflow {
				t.Errorf("SafeCastIntToUint32() error = %v, want %v", err, ErrIntegerOverflow)
			}
		})
	}
}

func TestValidateUsableSize(t *testing.T) {
	tests := []struct {
		name      string
		input     uint32
		wantError bool
	}{
		{
			name:      "valid - minimum size",
			input:     MinUsableSize,
			wantError: false,
		},
		{
			name:      "valid - large size",
			input:     4096,
			wantError: false,
		},
		{
			name:      "invalid - too small",
			input:     MinUsableSize - 1,
			wantError: true,
		},
		{
			name:      "invalid - zero",
			input:     0,
			wantError: true,
		},
		{
			name:      "invalid - very small",
			input:     10,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUsableSize(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateUsableSize() error = %v, wantError %v", err, tt.wantError)
			}
			if tt.wantError && err != ErrIntegerUnderflow {
				t.Errorf("ValidateUsableSize() error = %v, want %v", err, ErrIntegerUnderflow)
			}
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkSafeCastUint32ToUint16(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SafeCastUint32ToUint16(uint32(i % math.MaxUint16))
	}
}

func BenchmarkSafeAddUint32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SafeAddUint32(uint32(i), 1000)
	}
}

func BenchmarkSafeSubUint32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SafeSubUint32(math.MaxUint32, uint32(i%1000))
	}
}
