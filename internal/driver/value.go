package driver

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

// ValueConverter provides type conversion utilities for SQLite values.
type ValueConverter struct{}

func isNativeDriverValue(v interface{}) bool {
	switch v.(type) {
	case int64, float64, bool, []byte, string, time.Time:
		return true
	}
	return false
}

func convertUint64(val uint64) (driver.Value, error) {
	if val > 1<<63-1 {
		return nil, fmt.Errorf("uint64 value %d overflows int64", val)
	}
	return int64(val), nil
}

// int64Kinds is the set of kinds that can be converted to int64.
var int64Kinds = map[reflect.Kind]bool{
	reflect.Int: true, reflect.Int8: true, reflect.Int16: true, reflect.Int32: true,
	reflect.Uint: true, reflect.Uint8: true, reflect.Uint16: true, reflect.Uint32: true,
}

func convertToInt64(v interface{}) (int64, bool) {
	rv := reflect.ValueOf(v)
	if !int64Kinds[rv.Kind()] {
		return 0, false
	}
	if rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint32 {
		uval := rv.Uint()
		// Uint32 and smaller always fit in int64
		return int64(uval), true
	}
	return rv.Int(), true
}

func (vc ValueConverter) ConvertValue(v interface{}) (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	if isNativeDriverValue(v) {
		return v, nil
	}
	if i64, ok := convertToInt64(v); ok {
		return i64, nil
	}
	if u64, ok := v.(uint64); ok {
		return convertUint64(u64)
	}
	if f32, ok := v.(float32); ok {
		return float64(f32), nil
	}
	return nil, fmt.Errorf("unsupported type: %T", v)
}

// Result implements database/sql/driver.Result.
type Result struct {
	lastInsertID int64
	rowsAffected int64
}

// LastInsertId returns the last inserted ID.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// sqliteValueConverter is the default value converter for SQLite.
var sqliteValueConverter = ValueConverter{}
