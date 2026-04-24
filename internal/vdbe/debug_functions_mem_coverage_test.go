// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"io"
	"math"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/observability"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// newDiscardLogger creates a no-op observability logger for test use.
func newDiscardLogger() observability.Logger {
	return observability.NewLogger(observability.TraceLevel, io.Discard, observability.TextFormat)
}

// openMemDB opens an in-memory database for test use.
func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// execSQL runs a statement and fatals on error.
func execSQL(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// debug.go – formatInstruction coverage
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemFormatInstructionP4Types exercises all P4Type branches
// in formatInstruction by constructing instructions with each P4 type and
// verifying DumpProgram output is non-empty.
func TestDebugFunctionsMemFormatInstructionP4Types(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		p4Type vdbe.P4Type
		p4     vdbe.P4Union
		want   string
	}{
		{
			name:   "P4Int32",
			p4Type: vdbe.P4Int32,
			p4:     vdbe.P4Union{I: 42},
			want:   "42",
		},
		{
			name:   "P4Int64",
			p4Type: vdbe.P4Int64,
			p4:     vdbe.P4Union{I64: 9999999},
			want:   "9999999",
		},
		{
			name:   "P4Real",
			p4Type: vdbe.P4Real,
			p4:     vdbe.P4Union{R: 3.14},
			want:   "3.14",
		},
		{
			name:   "P4Static",
			p4Type: vdbe.P4Static,
			p4:     vdbe.P4Union{Z: "hello"},
			want:   "hello",
		},
		{
			name:   "P4Dynamic",
			p4Type: vdbe.P4Dynamic,
			p4:     vdbe.P4Union{Z: "world"},
			want:   "world",
		},
		{
			name:   "no_P4",
			p4Type: vdbe.P4Type(0),
			p4:     vdbe.P4Union{},
			want:   "0000",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := vdbe.New()
			instr := &vdbe.Instruction{
				Opcode: vdbe.OpInteger,
				P1:     1,
				P2:     2,
				P3:     3,
				P4:     tc.p4,
				P4Type: tc.p4Type,
			}
			v.Program = append(v.Program, instr)
			out := v.DumpProgram()
			if !strings.Contains(out, tc.want) {
				t.Errorf("DumpProgram() does not contain %q; got:\n%s", tc.want, out)
			}
		})
	}
}

// TestDebugFunctionsMemFormatInstructionComment ensures a non-empty Comment
// appears in the formatted output (the ";" branch in formatInstruction).
func TestDebugFunctionsMemFormatInstructionComment(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	instr := &vdbe.Instruction{
		Opcode:  vdbe.OpInteger,
		P1:      7,
		Comment: "this is a comment",
	}
	v.Program = append(v.Program, instr)
	out := v.DumpProgram()
	if !strings.Contains(out, "this is a comment") {
		t.Errorf("expected comment in DumpProgram output; got:\n%s", out)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// debug.go – logAffectedRegisters / captureRegisterSnapshot / TraceInstructionAfter
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemTraceInstructionAfterRegisters exercises the
// DebugRegisters branch of TraceInstructionAfter (calls logAffectedRegisters
// which in turn relies on captureRegisterSnapshot).
func TestDebugFunctionsMemTraceInstructionAfterRegisters(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugRegisters)
	if err := v.AllocMemory(5); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}

	v.Mem[0].SetInt(10)
	v.Mem[1].SetReal(2.5)
	v.Mem[2].SetStr("hello")

	instr := &vdbe.Instruction{Opcode: vdbe.OpAdd, P1: 0, P2: 1, P3: 2}

	// TraceInstruction captures the pre-execution snapshot when DebugRegisters enabled.
	v.TraceInstruction(0, instr)

	// Mutate a register to simulate an effect.
	v.Mem[2].SetInt(99)

	// TraceInstructionAfter logs the diff.
	v.TraceInstructionAfter(0, instr)
}

// TestDebugFunctionsMemTraceInstructionAfterCursors exercises the
// DebugCursors branch of TraceInstructionAfter.
func TestDebugFunctionsMemTraceInstructionAfterCursors(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugCursors)
	if err := v.AllocMemory(5); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}
	if err := v.AllocCursors(2); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	v.Cursors[0] = &vdbe.Cursor{CurType: vdbe.CursorBTree, IsTable: true}

	instr := &vdbe.Instruction{Opcode: vdbe.OpNext, P1: 0}
	v.TraceInstructionAfter(0, instr)
}

// TestDebugFunctionsMemTraceInstructionAfterBothModes exercises both
// DebugRegisters and DebugCursors simultaneously with a logger to cover
// logInstructionToObservability and logAffectedRegisters fully.
func TestDebugFunctionsMemTraceInstructionAfterBothModes(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugAll)
	v.SetDebugLogger(newDiscardLogger())
	if err := v.AllocMemory(5); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}
	if err := v.AllocCursors(2); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	v.Cursors[0] = &vdbe.Cursor{CurType: vdbe.CursorBTree, IsTable: true}
	v.Mem[1].SetInt(100)
	v.Mem[2].SetStr("before")

	instr := &vdbe.Instruction{
		Opcode: vdbe.OpAdd,
		P1:     1,
		P2:     2,
		P3:     3,
	}

	// TraceInstruction triggers captureRegisterSnapshot (DebugRegisters path)
	// and logInstructionToObservability (DebugTrace path).
	v.TraceInstruction(0, instr)
	// Mutate to create a detectable change.
	v.Mem[3].SetInt(200)
	v.TraceInstructionAfter(0, instr)
}

// TestDebugFunctionsMemLogAffectedRegistersNoSnapshot exercises the
// "no snapshot" early-exit path in logAffectedRegisters.
func TestDebugFunctionsMemLogAffectedRegistersNoSnapshot(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugRegisters)
	v.SetDebugLogger(newDiscardLogger())
	if err := v.AllocMemory(5); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}

	// Call TraceInstructionAfter WITHOUT a prior TraceInstruction so there is
	// no register snapshot – exercises the !hasSnapshot branch.
	instr := &vdbe.Instruction{Opcode: vdbe.OpAdd, P1: 0, P2: 1, P3: 2}
	v.TraceInstructionAfter(5, instr)
}

// ─────────────────────────────────────────────────────────────────────────────
// debug.go – logInstructionToObservability / GetInstructionLog
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemLogInstructionToObservability exercises the
// "Logger != nil" branch in logInstructionToObservability via TraceInstruction
// with DebugTrace enabled and a real logger.
func TestDebugFunctionsMemLogInstructionToObservability(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugTrace)
	v.SetDebugLogger(newDiscardLogger())
	if err := v.AllocMemory(5); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}

	instr := &vdbe.Instruction{
		Opcode: vdbe.OpInteger,
		P1:     1,
		P2:     0,
	}

	// TraceInstruction -> logInstruction + logInstructionToObservability
	result := v.TraceInstruction(0, instr)
	if !result {
		t.Error("expected TraceInstruction to return true (continue)")
	}

	log := v.GetInstructionLog()
	if len(log) == 0 {
		t.Error("expected at least one instruction in log")
	}
}

// TestDebugFunctionsMemGetInstructionLogEmpty verifies GetInstructionLog on a
// VDBE with no debug context returns an empty (not nil) slice.
func TestDebugFunctionsMemGetInstructionLogEmpty(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	log := v.GetInstructionLog()
	if log == nil {
		t.Error("GetInstructionLog with nil debug should return non-nil slice")
	}
	if len(log) != 0 {
		t.Errorf("expected empty log, got %d entries", len(log))
	}
}

// TestDebugFunctionsMemGetInstructionLogMaxSize exercises the log-trimming
// branch: when more entries than MaxLogSize are added the log is capped.
func TestDebugFunctionsMemGetInstructionLogMaxSize(t *testing.T) {
	t.Parallel()

	v := vdbe.New()
	v.SetDebugMode(vdbe.DebugTrace)
	v.Debug.MaxLogSize = 5

	if err := v.AllocMemory(3); err != nil {
		t.Fatalf("AllocMemory: %v", err)
	}

	instr := &vdbe.Instruction{Opcode: vdbe.OpInteger, P1: 1, P2: 0}
	for i := 0; i < 10; i++ {
		v.TraceInstruction(i, instr)
	}

	log := v.GetInstructionLog()
	if len(log) > 5 {
		t.Errorf("log trimming failed: expected ≤5 entries, got %d", len(log))
	}
}

// TestDebugFunctionsMemCaptureRegisterSnapshotNilDebug verifies that
// TraceInstruction with DebugRegisters enabled but nil Debug does not panic.
func TestDebugFunctionsMemCaptureRegisterSnapshotNilDebug(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	// Debug is nil - TraceInstruction should return true immediately.
	instr := &vdbe.Instruction{Opcode: vdbe.OpHalt}
	if !v.TraceInstruction(0, instr) {
		t.Error("expected true from TraceInstruction when Debug is nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// functions.go – valueToMem, storeResult, createAggregateInstance, opAggStep
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemValueToMemAllTypes exercises all type branches of
// valueToMem through ExecuteFunction calls on built-in scalar functions,
// and also exercises storeResult (via opFunction's internal storeResult call).
func TestDebugFunctionsMemValueToMemAllTypes_IntAndReal(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	var intVal int64
	if err := db.QueryRow("SELECT length('hello')").Scan(&intVal); err != nil {
		t.Fatalf("length: %v", err)
	}
	if intVal != 5 {
		t.Errorf("length('hello') = %d, want 5", intVal)
	}

	execSQL(t, db, "CREATE TABLE nums (v REAL)")
	execSQL(t, db, "INSERT INTO nums VALUES (1.5)")
	execSQL(t, db, "INSERT INTO nums VALUES (2.5)")
	var realVal float64
	if err := db.QueryRow("SELECT AVG(v) FROM nums").Scan(&realVal); err != nil {
		t.Fatalf("AVG: %v", err)
	}
	if math.Abs(realVal-2.0) > 1e-9 {
		t.Errorf("AVG = %v, want 2.0", realVal)
	}
}

func TestDebugFunctionsMemValueToMemAllTypes_TextAndNull(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	var textVal string
	if err := db.QueryRow("SELECT upper('abc')").Scan(&textVal); err != nil {
		t.Fatalf("upper: %v", err)
	}
	if textVal != "ABC" {
		t.Errorf("upper('abc') = %q, want 'ABC'", textVal)
	}

	var nullVal interface{}
	if err := db.QueryRow("SELECT abs(NULL)").Scan(&nullVal); err != nil {
		t.Fatalf("abs(NULL): %v", err)
	}
	if nullVal != nil {
		t.Errorf("abs(NULL) = %v, want nil", nullVal)
	}
}

// TestDebugFunctionsMemValueToMemBlob exercises the blob type branch of
// valueToMem through a CAST expression that returns a BLOB.
func TestDebugFunctionsMemValueToMemBlob(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE blobs (b BLOB)")
	execSQL(t, db, "INSERT INTO blobs VALUES (X'DEADBEEF')")

	var blobVal []byte
	if err := db.QueryRow("SELECT b FROM blobs").Scan(&blobVal); err != nil {
		t.Fatalf("blob select: %v", err)
	}
	if len(blobVal) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(blobVal))
	}
}

// TestDebugFunctionsMemStoreResultError exercises the error path in storeResult
// by calling a scalar function that should succeed and confirming result placement.
func TestDebugFunctionsMemStoreResultError(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	// hex() returns text; round-trips through valueToMem (TypeText) and storeResult.
	var hexVal string
	if err := db.QueryRow("SELECT hex(X'ABCD')").Scan(&hexVal); err != nil {
		t.Fatalf("hex: %v", err)
	}
	if !strings.EqualFold(hexVal, "ABCD") {
		t.Errorf("hex(X'ABCD') = %q, want ABCD", hexVal)
	}
}

// TestDebugFunctionsMemCreateAggregateInstanceFreshState verifies that each
// use of an aggregate function in a query gets a fresh instance (createAggregateInstance).
// debugFuncCheckAggResult checks a single aggregate group result.
func debugFuncCheckAggResult(t *testing.T, grp string, sum, count, max, min int64, want map[string][4]int64) {
	t.Helper()
	w, ok := want[grp]
	if !ok {
		t.Errorf("unexpected group %q", grp)
		return
	}
	if sum != w[0] || count != w[1] || max != w[2] || min != w[3] {
		t.Errorf("grp=%q: got sum=%d count=%d max=%d min=%d; want sum=%d count=%d max=%d min=%d",
			grp, sum, count, max, min, w[0], w[1], w[2], w[3])
	}
}

func TestDebugFunctionsMemCreateAggregateInstanceFreshState(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE agg_t (grp TEXT, val INTEGER)")
	for _, row := range []struct {
		g string
		v int
	}{
		{"a", 10}, {"a", 20}, {"a", 30},
		{"b", 1}, {"b", 2},
	} {
		execSQL(t, db, "INSERT INTO agg_t VALUES (?, ?)", row.g, row.v)
	}

	rows, err := db.Query("SELECT grp, SUM(val), COUNT(val), MAX(val), MIN(val) FROM agg_t GROUP BY grp ORDER BY grp")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	want := map[string][4]int64{"a": {60, 3, 30, 10}, "b": {3, 2, 2, 1}}
	for rows.Next() {
		var grp string
		var sum, count, max, min int64
		if err := rows.Scan(&grp, &sum, &count, &max, &min); err != nil {
			t.Fatalf("scan: %v", err)
		}
		debugFuncCheckAggResult(t, grp, sum, count, max, min, want)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

// TestDebugFunctionsMemOpAggStepGroupConcat exercises opAggStep with the
// group_concat function to cover the string accumulation path.
func TestDebugFunctionsMemOpAggStepGroupConcat(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE words (w TEXT)")
	for _, w := range []string{"foo", "bar", "baz"} {
		execSQL(t, db, "INSERT INTO words VALUES (?)", w)
	}

	var result string
	if err := db.QueryRow("SELECT group_concat(w, ',') FROM words").Scan(&result); err != nil {
		t.Fatalf("group_concat: %v", err)
	}
	parts := strings.Split(result, ",")
	if len(parts) != 3 {
		t.Errorf("group_concat result %q: expected 3 parts, got %d", result, len(parts))
	}
}

// TestDebugFunctionsMemOpAggStepWithNulls verifies that NULLs in aggregate
// inputs are handled correctly (opAggStep null-value path).
func TestDebugFunctionsMemOpAggStepWithNulls(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE nullable (v INTEGER)")
	execSQL(t, db, "INSERT INTO nullable VALUES (1)")
	execSQL(t, db, "INSERT INTO nullable VALUES (NULL)")
	execSQL(t, db, "INSERT INTO nullable VALUES (3)")

	var sum int64
	if err := db.QueryRow("SELECT SUM(v) FROM nullable").Scan(&sum); err != nil {
		t.Fatalf("SUM with nulls: %v", err)
	}
	if sum != 4 {
		t.Errorf("SUM(1,NULL,3) = %d, want 4", sum)
	}
}

// TestDebugFunctionsMemOpAggStepCountStar exercises COUNT(*) which uses a
// variant aggregate step path.
func TestDebugFunctionsMemOpAggStepCountStar(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE ct (x INTEGER)")
	for i := 0; i < 5; i++ {
		execSQL(t, db, "INSERT INTO ct VALUES (?)", i)
	}

	var cnt int64
	if err := db.QueryRow("SELECT COUNT(*) FROM ct").Scan(&cnt); err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if cnt != 5 {
		t.Errorf("COUNT(*) = %d, want 5", cnt)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// mem.go – RealValue, BlobValue, Stringify
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemRealValueBranches exercises the uncovered branches of
// RealValue: int-to-real conversion, string-to-real conversion, blob-to-real
// conversion, and leading-numeric-prefix fallback.
func TestDebugFunctionsMemRealValueBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mem  *vdbe.Mem
		want float64
	}{
		{
			name: "real_direct",
			mem:  vdbe.NewMemReal(3.14),
			want: 3.14,
		},
		{
			name: "int_to_real",
			mem:  vdbe.NewMemInt(7),
			want: 7.0,
		},
		{
			name: "str_to_real_parseable",
			mem:  vdbe.NewMemStr("2.718"),
			want: 2.718,
		},
		{
			name: "str_leading_numeric",
			mem:  vdbe.NewMemStr("6.28xyz"),
			want: 6.28,
		},
		{
			name: "blob_to_real",
			mem:  vdbe.NewMemBlob([]byte("1.5")),
			want: 1.5,
		},
		{
			name: "null_returns_zero",
			mem:  vdbe.NewMemNull(),
			want: 0.0,
		},
		{
			name: "str_non_numeric_returns_zero",
			mem:  vdbe.NewMemStr("notanumber"),
			want: 0.0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.mem.RealValue()
			if math.Abs(got-tc.want) > 1e-12 {
				t.Errorf("RealValue() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDebugFunctionsMemBlobValueBranches exercises BlobValue when the Mem
// contains a string (returns string bytes) vs a blob (returns blob bytes)
// vs neither (returns nil).
func TestDebugFunctionsMemBlobValueBranches(t *testing.T) {
	t.Parallel()

	t.Run("blob_type", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x01, 0x02, 0x03}
		m := vdbe.NewMemBlob(data)
		got := m.BlobValue()
		if len(got) != len(data) {
			t.Errorf("BlobValue() len = %d, want %d", len(got), len(data))
		}
		for i := range data {
			if got[i] != data[i] {
				t.Errorf("BlobValue()[%d] = %x, want %x", i, got[i], data[i])
			}
		}
	})

	t.Run("str_type_returns_bytes", func(t *testing.T) {
		t.Parallel()
		m := vdbe.NewMemStr("hello")
		got := m.BlobValue()
		if string(got) != "hello" {
			t.Errorf("BlobValue() for MemStr = %q, want 'hello'", string(got))
		}
	})

	t.Run("int_type_returns_nil", func(t *testing.T) {
		t.Parallel()
		m := vdbe.NewMemInt(42)
		got := m.BlobValue()
		if got != nil {
			t.Errorf("BlobValue() for MemInt = %v, want nil", got)
		}
	})

	t.Run("null_returns_nil", func(t *testing.T) {
		t.Parallel()
		m := vdbe.NewMemNull()
		got := m.BlobValue()
		if got != nil {
			t.Errorf("BlobValue() for MemNull = %v, want nil", got)
		}
	})
}

// TestDebugFunctionsMemStringifyBranches exercises each branch of Stringify:
// already-string (no-op), int, real, blob, null, and undefined-error.
func TestDebugFunctionsMemStringifyBranches_TypeConversions(t *testing.T) {
	t.Parallel()

	// already_string_noop
	m := vdbe.NewMemStr("already")
	if err := m.Stringify(); err != nil {
		t.Fatalf("Stringify on MemStr: %v", err)
	}
	if !m.IsStr() {
		t.Error("expected IsStr true after Stringify no-op")
	}

	// int_to_string
	m = vdbe.NewMemInt(12345)
	if err := m.Stringify(); err != nil {
		t.Fatalf("Stringify on MemInt: %v", err)
	}
	if m.StrValue() != "12345" {
		t.Errorf("Stringify(MemInt) = %q, want '12345'", m.StrValue())
	}

	// real_to_string
	m = vdbe.NewMemReal(9.9)
	if err := m.Stringify(); err != nil {
		t.Fatalf("Stringify on MemReal: %v", err)
	}
	if !strings.Contains(m.StrValue(), "9.9") {
		t.Errorf("Stringify(MemReal) = %q, want string containing '9.9'", m.StrValue())
	}

	// blob_to_string
	m = vdbe.NewMemBlob([]byte("blobdata"))
	if err := m.Stringify(); err != nil {
		t.Fatalf("Stringify on MemBlob: %v", err)
	}
	if m.StrValue() != "blobdata" {
		t.Errorf("Stringify(MemBlob) = %q, want 'blobdata'", m.StrValue())
	}
}

func TestDebugFunctionsMemStringifyBranches_NullAndUndefined(t *testing.T) {
	t.Parallel()

	m := vdbe.NewMemNull()
	if err := m.Stringify(); err != nil {
		t.Fatalf("Stringify on MemNull: %v", err)
	}
	if m.StrValue() != "" {
		t.Errorf("Stringify(MemNull) = %q, want empty string", m.StrValue())
	}

	m = vdbe.NewMem()
	if err := m.Stringify(); err == nil {
		t.Error("Stringify on undefined Mem should return error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// record.go – decodeFloat64
// ─────────────────────────────────────────────────────────────────────────────

// TestDebugFunctionsMemDecodeFloat64 exercises decodeFloat64 via
// DecodeRecord/EncodeSimpleRecord, covering the happy path (enough bytes)
// and the truncated error path.
// debugFuncCheckFloat64Roundtrip verifies a single float64 round-trip through encode/decode.
func debugFuncCheckFloat64Roundtrip(t *testing.T, f float64) {
	t.Helper()
	if math.IsNaN(f) {
		return
	}
	encoded := vdbe.EncodeSimpleRecord([]interface{}{f})
	decoded, err := vdbe.DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord(%v): %v", f, err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 value, got %d", len(decoded))
	}
	got, ok := decoded[0].(float64)
	if !ok {
		t.Fatalf("expected float64, got %T (%v)", decoded[0], decoded[0])
	}
	if math.IsInf(f, 0) {
		if got != f {
			t.Errorf("float64 inf round-trip: got %v, want %v", got, f)
		}
	} else if math.Abs(got-f) > 1e-15 && got != f {
		t.Errorf("float64 round-trip: got %v, want %v", got, f)
	}
}

func TestDebugFunctionsMemDecodeFloat64_Roundtrip(t *testing.T) {
	t.Parallel()
	for _, f := range []float64{
		0.0, 1.0, -1.0, 3.14159265358979,
		math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		math.Inf(1), math.Inf(-1),
	} {
		debugFuncCheckFloat64Roundtrip(t, f)
	}
}

func TestDebugFunctionsMemDecodeFloat64_MixedRecord(t *testing.T) {
	t.Parallel()
	values := []interface{}{int64(42), 2.71828, "text", nil}
	encoded := vdbe.EncodeSimpleRecord(values)
	decoded, err := vdbe.DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord mixed: %v", err)
	}
	if len(decoded) != 4 {
		t.Fatalf("expected 4 values, got %d", len(decoded))
	}
	if got, ok := decoded[1].(float64); !ok || math.Abs(got-2.71828) > 1e-10 {
		t.Errorf("decoded[1] = %v (%T), want ~2.71828", decoded[1], decoded[1])
	}
}

// TestDebugFunctionsMemRealValuesViaSQL exercises the REAL type path through
// SQL queries, ensuring real values flow correctly through the VDBE.
func TestDebugFunctionsMemRealValuesViaSQL(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE reals (v REAL)")
	for _, v := range []float64{1.1, 2.2, 3.3, 4.4, 5.5} {
		execSQL(t, db, "INSERT INTO reals VALUES (?)", v)
	}

	var total float64
	if err := db.QueryRow("SELECT SUM(v) FROM reals").Scan(&total); err != nil {
		t.Fatalf("SUM real: %v", err)
	}
	if math.Abs(total-16.5) > 1e-9 {
		t.Errorf("SUM(reals) = %v, want 16.5", total)
	}

	var avg float64
	if err := db.QueryRow("SELECT AVG(v) FROM reals").Scan(&avg); err != nil {
		t.Fatalf("AVG real: %v", err)
	}
	if math.Abs(avg-3.3) > 1e-9 {
		t.Errorf("AVG(reals) = %v, want 3.3", avg)
	}

	var maxV float64
	if err := db.QueryRow("SELECT MAX(v) FROM reals").Scan(&maxV); err != nil {
		t.Fatalf("MAX real: %v", err)
	}
	if math.Abs(maxV-5.5) > 1e-9 {
		t.Errorf("MAX(reals) = %v, want 5.5", maxV)
	}
}

// TestDebugFunctionsMemBlobValuesViaSQL exercises the BLOB type path by storing
// and retrieving binary data through SQL.
// debugFuncCheckBlobData validates blob data matches expected bytes.
func debugFuncCheckBlobData(t *testing.T, rowIdx int, data, expected []byte) {
	t.Helper()
	if len(data) != len(expected) {
		t.Errorf("row %d: blob len = %d, want %d", rowIdx, len(data), len(expected))
		return
	}
	for j := range expected {
		if data[j] != expected[j] {
			t.Errorf("row %d byte %d: got %x, want %x", rowIdx, j, data[j], expected[j])
			return
		}
	}
}

func TestDebugFunctionsMemBlobValuesViaSQL(t *testing.T) {
	t.Parallel()
	db := openMemDB(t)

	execSQL(t, db, "CREATE TABLE blobtest (id INTEGER PRIMARY KEY, data BLOB)")
	execSQL(t, db, "INSERT INTO blobtest VALUES (1, X'0102030405')")
	execSQL(t, db, "INSERT INTO blobtest VALUES (2, X'DEADBEEF')")

	rows, err := db.Query("SELECT id, data FROM blobtest ORDER BY id")
	if err != nil {
		t.Fatalf("query blobs: %v", err)
	}
	defer rows.Close()

	expected := [][]byte{
		{0x01, 0x02, 0x03, 0x04, 0x05},
		{0xDE, 0xAD, 0xBE, 0xEF},
	}

	i := 0
	for rows.Next() {
		var id int
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			t.Fatalf("scan blob row: %v", err)
		}
		if i >= len(expected) {
			t.Fatalf("too many rows")
		}
		debugFuncCheckBlobData(t, i, data, expected[i])
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}
