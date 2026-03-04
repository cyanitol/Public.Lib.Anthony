package driver

import "github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"

// registerExtractor is a function that extracts register indices from an instruction
type registerExtractor func(*vdbe.Instruction) []int

// extractRegsP1P2 returns registers from P1 and P2
func extractRegsP1P2(instr *vdbe.Instruction) []int {
	return []int{instr.P1, instr.P2}
}

// extractRegsP1P2P3 returns registers from P1, P2, and P3
func extractRegsP1P2P3(instr *vdbe.Instruction) []int {
	return []int{instr.P1, instr.P2, instr.P3}
}

// extractRegsP3 returns register from P3
func extractRegsP3(instr *vdbe.Instruction) []int {
	return []int{instr.P3}
}

// extractRegsP2 returns register from P2
func extractRegsP2(instr *vdbe.Instruction) []int {
	return []int{instr.P2}
}

// extractRegsP1P3 returns registers from P1 and P3
func extractRegsP1P3(instr *vdbe.Instruction) []int {
	return []int{instr.P1, instr.P3}
}

// extractRegsP1 returns register from P1
func extractRegsP1(instr *vdbe.Instruction) []int {
	return []int{instr.P1}
}

// extractRegsResultRow returns the range of registers for ResultRow
func extractRegsResultRow(instr *vdbe.Instruction) []int {
	return []int{instr.P1 + instr.P2 - 1}
}

// extractRegsFunction returns registers for function calls including argument range
func extractRegsFunction(instr *vdbe.Instruction) []int {
	regs := []int{instr.P2, instr.P3}
	if instr.P2 > 0 && instr.P5 > 0 {
		regs = append(regs, instr.P2+int(instr.P5)-1)
	}
	return regs
}

// opcodeRegisterExtractors maps opcodes to their register extractor functions
var opcodeRegisterExtractors = map[vdbe.Opcode]registerExtractor{
	// P1, P2 extractors
	vdbe.OpMove: extractRegsP1P2, vdbe.OpCopy: extractRegsP1P2, vdbe.OpSCopy: extractRegsP1P2,
	vdbe.OpNot: extractRegsP1P2, vdbe.OpBitNot: extractRegsP1P2, vdbe.OpCast: extractRegsP1P2,

	// P1, P2, P3 extractors
	vdbe.OpAdd: extractRegsP1P2P3, vdbe.OpSubtract: extractRegsP1P2P3, vdbe.OpMultiply: extractRegsP1P2P3,
	vdbe.OpDivide: extractRegsP1P2P3, vdbe.OpRemainder: extractRegsP1P2P3, vdbe.OpConcat: extractRegsP1P2P3,
	vdbe.OpBitAnd: extractRegsP1P2P3, vdbe.OpBitOr: extractRegsP1P2P3, vdbe.OpShiftLeft: extractRegsP1P2P3,
	vdbe.OpShiftRight: extractRegsP1P2P3, vdbe.OpEq: extractRegsP1P2P3, vdbe.OpNe: extractRegsP1P2P3,
	vdbe.OpLt: extractRegsP1P2P3, vdbe.OpLe: extractRegsP1P2P3, vdbe.OpGt: extractRegsP1P2P3,
	vdbe.OpGe: extractRegsP1P2P3, vdbe.OpAnd: extractRegsP1P2P3, vdbe.OpOr: extractRegsP1P2P3,

	// P3 extractors
	vdbe.OpColumn: extractRegsP3,

	// P2 extractors
	vdbe.OpInteger: extractRegsP2, vdbe.OpString8: extractRegsP2, vdbe.OpNull: extractRegsP2,
	vdbe.OpRowid: extractRegsP2, vdbe.OpReal: extractRegsP2, vdbe.OpInt64: extractRegsP2,
	vdbe.OpBlob: extractRegsP2, vdbe.OpSorterData: extractRegsP2,

	// Special extractors
	vdbe.OpResultRow: extractRegsResultRow,

	// P1, P3 extractors
	vdbe.OpMakeRecord: extractRegsP1P3, vdbe.OpInsert: extractRegsP1P3,
	vdbe.OpSorterInsert: extractRegsP1P3, vdbe.OpAggDistinct: extractRegsP1P3,

	// Function extractors
	vdbe.OpFunction: extractRegsFunction, vdbe.OpAggStep: extractRegsFunction,

	// P1 extractors
	vdbe.OpIf: extractRegsP1, vdbe.OpIfNot: extractRegsP1, vdbe.OpIfPos: extractRegsP1,
	vdbe.OpIfNotZero: extractRegsP1, vdbe.OpIsNull: extractRegsP1, vdbe.OpNotNull: extractRegsP1,
	vdbe.OpGosub: extractRegsP1, vdbe.OpReturn: extractRegsP1, vdbe.OpInitCoroutine: extractRegsP1,
	vdbe.OpEndCoroutine: extractRegsP1, vdbe.OpYield: extractRegsP1, vdbe.OpAggFinal: extractRegsP1,
	vdbe.OpAddImm: extractRegsP1,
}

// updateMaxFromRegs updates maxReg if any value in regs is larger
func updateMaxFromRegs(maxReg *int, regs []int) {
	for _, r := range regs {
		if r > *maxReg {
			*maxReg = r
		}
	}
}

// registerAdjuster is a function that adjusts register indices in an instruction
type registerAdjuster func(*vdbe.Instruction, int)

// adjustRegsP1P2 adjusts registers P1 and P2
func adjustRegsP1P2(instr *vdbe.Instruction, offset int) {
	instr.P1 += offset
	instr.P2 += offset
}

// adjustRegsP1P2P3 adjusts registers P1, P2, and P3
func adjustRegsP1P2P3(instr *vdbe.Instruction, offset int) {
	instr.P1 += offset
	instr.P2 += offset
	instr.P3 += offset
}

// adjustRegsP3 adjusts register P3
func adjustRegsP3(instr *vdbe.Instruction, offset int) {
	instr.P3 += offset
}

// adjustRegsP2 adjusts register P2
func adjustRegsP2(instr *vdbe.Instruction, offset int) {
	instr.P2 += offset
}

// adjustRegsP1 adjusts register P1
func adjustRegsP1(instr *vdbe.Instruction, offset int) {
	instr.P1 += offset
}

// adjustRegsP1P3 adjusts registers P1 and P3
func adjustRegsP1P3(instr *vdbe.Instruction, offset int) {
	instr.P1 += offset
	instr.P3 += offset
}

// adjustRegsP2P3 adjusts registers P2 and P3
func adjustRegsP2P3(instr *vdbe.Instruction, offset int) {
	instr.P2 += offset
	instr.P3 += offset
}

// opcodeRegisterAdjusters maps opcodes to their register adjuster functions
var opcodeRegisterAdjusters = map[vdbe.Opcode]registerAdjuster{
	// P1, P2 adjusters
	vdbe.OpMove: adjustRegsP1P2, vdbe.OpCopy: adjustRegsP1P2, vdbe.OpSCopy: adjustRegsP1P2,
	vdbe.OpNot: adjustRegsP1P2, vdbe.OpBitNot: adjustRegsP1P2, vdbe.OpCast: adjustRegsP1P2,

	// P1, P2, P3 adjusters
	vdbe.OpAdd: adjustRegsP1P2P3, vdbe.OpSubtract: adjustRegsP1P2P3, vdbe.OpMultiply: adjustRegsP1P2P3,
	vdbe.OpDivide: adjustRegsP1P2P3, vdbe.OpRemainder: adjustRegsP1P2P3, vdbe.OpConcat: adjustRegsP1P2P3,
	vdbe.OpBitAnd: adjustRegsP1P2P3, vdbe.OpBitOr: adjustRegsP1P2P3, vdbe.OpShiftLeft: adjustRegsP1P2P3,
	vdbe.OpShiftRight: adjustRegsP1P2P3, vdbe.OpEq: adjustRegsP1P2P3, vdbe.OpNe: adjustRegsP1P2P3,
	vdbe.OpLt: adjustRegsP1P2P3, vdbe.OpLe: adjustRegsP1P2P3, vdbe.OpGt: adjustRegsP1P2P3,
	vdbe.OpGe: adjustRegsP1P2P3, vdbe.OpAnd: adjustRegsP1P2P3, vdbe.OpOr: adjustRegsP1P2P3,

	// P3 adjusters
	vdbe.OpColumn: adjustRegsP3,

	// P2 adjusters
	vdbe.OpInteger: adjustRegsP2, vdbe.OpString8: adjustRegsP2, vdbe.OpNull: adjustRegsP2,
	vdbe.OpRowid: adjustRegsP2, vdbe.OpReal: adjustRegsP2, vdbe.OpInt64: adjustRegsP2,
	vdbe.OpBlob: adjustRegsP2, vdbe.OpSorterData: adjustRegsP2,

	// P1, P3 adjusters
	vdbe.OpMakeRecord: adjustRegsP1P3, vdbe.OpAggDistinct: adjustRegsP1P3,

	// P2, P3 adjusters
	vdbe.OpFunction: adjustRegsP2P3, vdbe.OpAggStep: adjustRegsP2P3,
	vdbe.OpInsert: adjustRegsP2P3, vdbe.OpSorterInsert: adjustRegsP2P3,

	// P1 adjusters (note: some P2 are jump targets, not registers)
	vdbe.OpResultRow: adjustRegsP1, vdbe.OpAggFinal: adjustRegsP1,
	vdbe.OpIf: adjustRegsP1, vdbe.OpIfNot: adjustRegsP1, vdbe.OpIfPos: adjustRegsP1,
	vdbe.OpIfNotZero: adjustRegsP1, vdbe.OpIsNull: adjustRegsP1, vdbe.OpNotNull: adjustRegsP1,
	vdbe.OpGosub: adjustRegsP1, vdbe.OpReturn: adjustRegsP1, vdbe.OpInitCoroutine: adjustRegsP1,
	vdbe.OpEndCoroutine: adjustRegsP1, vdbe.OpYield: adjustRegsP1, vdbe.OpAddImm: adjustRegsP1,
}
