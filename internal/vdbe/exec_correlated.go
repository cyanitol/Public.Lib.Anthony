// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import "fmt"

// CorrelatedExistsFunc is called at runtime for correlated EXISTS subqueries.
// It receives the binding values from the outer row and returns whether any
// rows matched (true/false).
type CorrelatedExistsFunc func(bindings []interface{}) (bool, error)

// CorrelatedScalarFunc is called at runtime for correlated scalar subqueries.
// It receives the binding values from the outer row and returns the scalar
// result (e.g. COUNT(*) value).
type CorrelatedScalarFunc func(bindings []interface{}) (interface{}, error)

// execCorrelatedExists implements OpCorrelatedExists.
// P1 = result register, P2 = first binding register, P3 = num bindings,
// P4.P = CorrelatedExistsFunc, P5 = not flag (1 = NOT EXISTS).
func (v *VDBE) execCorrelatedExists(instr *Instruction) error {
	fn, ok := instr.P4.P.(CorrelatedExistsFunc)
	if !ok {
		return fmt.Errorf("CorrelatedExists: P4 is not CorrelatedExistsFunc")
	}

	bindings, err := v.collectBindings(instr.P2, instr.P3)
	if err != nil {
		return fmt.Errorf("CorrelatedExists: %w", err)
	}

	exists, err := fn(bindings)
	if err != nil {
		return fmt.Errorf("CorrelatedExists: %w", err)
	}

	return v.storeExistsResult(instr.P1, exists, instr.P5 != 0)
}

// execCorrelatedScalar implements OpCorrelatedScalar.
// P1 = result register, P2 = first binding register, P3 = num bindings,
// P4.P = CorrelatedScalarFunc.
func (v *VDBE) execCorrelatedScalar(instr *Instruction) error {
	fn, ok := instr.P4.P.(CorrelatedScalarFunc)
	if !ok {
		return fmt.Errorf("CorrelatedScalar: P4 is not CorrelatedScalarFunc")
	}

	bindings, err := v.collectBindings(instr.P2, instr.P3)
	if err != nil {
		return fmt.Errorf("CorrelatedScalar: %w", err)
	}

	result, err := fn(bindings)
	if err != nil {
		return fmt.Errorf("CorrelatedScalar: %w", err)
	}

	return v.storeScalarValue(instr.P1, result)
}

// collectBindings reads consecutive register values starting at firstReg.
func (v *VDBE) collectBindings(firstReg, count int) ([]interface{}, error) {
	bindings := make([]interface{}, count)
	for i := range count {
		mem, err := v.GetMem(firstReg + i)
		if err != nil {
			return nil, fmt.Errorf("binding register %d: %w", firstReg+i, err)
		}
		bindings[i] = mem.Value()
	}
	return bindings, nil
}

// storeExistsResult stores a boolean EXISTS result as 0 or 1 in a register.
func (v *VDBE) storeExistsResult(reg int, exists bool, negate bool) error {
	mem, err := v.GetMem(reg)
	if err != nil {
		return fmt.Errorf("result register %d: %w", reg, err)
	}
	val := int64(0)
	if exists != negate {
		val = 1
	}
	mem.SetInt(val)
	return nil
}

// storeScalarValue stores an arbitrary Go value into a VDBE register.
func (v *VDBE) storeScalarValue(reg int, val interface{}) error {
	mem, err := v.GetMem(reg)
	if err != nil {
		return fmt.Errorf("result register %d: %w", reg, err)
	}
	switch tv := val.(type) {
	case nil:
		mem.SetNull()
	case int64:
		mem.SetInt(tv)
	case float64:
		mem.SetReal(tv)
	case string:
		return mem.SetStr(tv)
	case []byte:
		return mem.SetBlob(tv)
	default:
		mem.SetNull()
	}
	return nil
}
