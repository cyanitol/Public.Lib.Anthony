# Bitwise and Logical Operations Quick Reference

## Bitwise Operations

### BitAnd - Bitwise AND
```
OpBitAnd: P3 = P1 & P2
```
- Converts operands to integers
- NULL + anything = NULL
- Example: 12 & 10 = 8 (binary: 1100 & 1010 = 1000)

### BitOr - Bitwise OR
```
OpBitOr: P3 = P1 | P2
```
- Converts operands to integers
- NULL + anything = NULL
- Example: 12 | 10 = 14 (binary: 1100 | 1010 = 1110)

### BitNot - Bitwise NOT
```
OpBitNot: P2 = ~P1
```
- Converts operand to integer
- NULL = NULL
- Example: ~15 = -16 (inverts all bits)

### ShiftLeft - Left Bit Shift
```
OpShiftLeft: P3 = P2 << P1
```
- P1 = shift amount, P2 = value
- Negative shift or shift >= 64 = 0
- NULL + anything = NULL
- Example: 8 << 2 = 32

### ShiftRight - Right Bit Shift (Arithmetic)
```
OpShiftRight: P3 = P2 >> P1
```
- P1 = shift amount, P2 = value
- Arithmetic shift (sign-extending)
- Negative shift = 0
- Shift >= 64: 0 for positive, -1 for negative
- NULL + anything = NULL
- Example: 32 >> 2 = 8, -32 >> 2 = -8

## Logical Operations (Three-Valued Logic)

### And - Logical AND
```
OpAnd: P3 = P1 AND P2
```
Returns: 0 (FALSE), 1 (TRUE), or NULL

Truth table:
| P1    | P2    | Result |
|-------|-------|--------|
| FALSE | any   | FALSE  |
| TRUE  | TRUE  | TRUE   |
| TRUE  | FALSE | FALSE  |
| NULL  | FALSE | FALSE  |
| NULL  | TRUE  | NULL   |
| NULL  | NULL  | NULL   |

### Or - Logical OR
```
OpOr: P3 = P1 OR P2
```
Returns: 0 (FALSE), 1 (TRUE), or NULL

Truth table:
| P1    | P2    | Result |
|-------|-------|--------|
| TRUE  | any   | TRUE   |
| FALSE | FALSE | FALSE  |
| FALSE | TRUE  | TRUE   |
| NULL  | TRUE  | TRUE   |
| NULL  | FALSE | NULL   |
| NULL  | NULL  | NULL   |

### Not - Logical NOT
```
OpNot: P2 = NOT P1
```
Returns: 0 (FALSE), 1 (TRUE), or NULL

Truth table:
| P1    | Result |
|-------|--------|
| TRUE  | FALSE  |
| FALSE | TRUE   |
| NULL  | NULL   |

## Boolean Evaluation Rules

For logical operations:
- 0 = FALSE
- Non-zero integer = TRUE
- Non-zero real = TRUE
- NULL = NULL

## Common Usage Patterns

### Testing bit flags
```
BitAnd to test: value & flag
BitOr to set: value | flag
BitNot to clear: value & ~flag
```

### Conditional logic
```
And for conjunctions: condition1 AND condition2
Or for disjunctions: condition1 OR condition2
Not for negation: NOT condition
```

### Bit manipulation
```
ShiftLeft to multiply by powers of 2
ShiftRight to divide by powers of 2
```
