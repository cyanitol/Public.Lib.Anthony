# Type Affinity in SQLite

> See [DATATYPES.md](DATATYPES.md) for the complete SQLite type system reference,
> and [TYPE_AFFINITY_SHORT.md](TYPE_AFFINITY_SHORT.md) for the quick affinity reference.

SQLite uses a flexible "type affinity" system rather than strict static typing. Each
column has an affinity that influences (but does not enforce) the storage class of values.

## The Five Affinities

| Affinity | Description |
|---|---|
| `TEXT` | Stores using storage classes NULL, TEXT, or BLOB |
| `NUMERIC` | May store as INTEGER or REAL if lossless, otherwise TEXT |
| `INTEGER` | Like NUMERIC but coerces to INTEGER for REAL with no fractional part |
| `REAL` | Like NUMERIC but coerces to REAL (floating point) |
| `BLOB` | No type coercion; stores exactly as provided |

## Affinity Determination

The affinity of a column is determined by its declared type:
- Contains "INT" → INTEGER affinity
- Contains "CHAR", "CLOB", or "TEXT" → TEXT affinity
- Contains "BLOB" or empty → BLOB affinity (none)
- Contains "REAL", "FLOA", or "DOUB" → REAL affinity
- Otherwise → NUMERIC affinity

Source: https://www.sqlite.org/datatype3.html
