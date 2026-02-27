package parser

// Example usage of ATTACH and DETACH parsing
//
// This file demonstrates how the parser handles ATTACH DATABASE and DETACH DATABASE statements.
//
// Example ATTACH statements:
//   ATTACH DATABASE 'mydb.db' AS mydb
//   ATTACH 'temp.db' AS temp
//   ATTACH DATABASE 'data/' || date() || '.db' AS daily
//
// Example DETACH statements:
//   DETACH DATABASE mydb
//   DETACH temp
//
// The implementation follows SQLite's syntax:
// - The DATABASE keyword is optional in both ATTACH and DETACH
// - ATTACH requires a filename expression (typically a string literal) and schema name
// - DETACH requires only the schema name
// - Schema names can be quoted identifiers
//
// AST Structure:
//
// AttachStmt:
//   - Filename: Expression (typically a LiteralExpr with LiteralString type)
//   - SchemaName: string (unquoted identifier)
//
// DetachStmt:
//   - SchemaName: string (unquoted identifier)
//
// Example code:
//
//   sql := "ATTACH DATABASE 'mydb.db' AS mydb"
//   parser := NewParser(sql)
//   stmts, err := parser.Parse()
//   if err != nil {
//       log.Fatal(err)
//   }
//   attachStmt := stmts[0].(*AttachStmt)
//   fmt.Printf("Attaching %v as %s\n", attachStmt.Filename, attachStmt.SchemaName)
