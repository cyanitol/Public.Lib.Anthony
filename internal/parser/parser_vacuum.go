// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

// parseVacuum parses a VACUUM statement.
// Syntax:
//   VACUUM
//   VACUUM schema_name
//   VACUUM INTO filename
//   VACUUM schema_name INTO filename
func (p *Parser) parseVacuum() (*VacuumStmt, error) {
	stmt := &VacuumStmt{}

	// Check if next token is an identifier (schema name) or INTO
	if p.check(TK_ID) {
		// Schema name
		stmt.Schema = p.consumeSchemaIdentifier()
	}

	// Check for INTO clause
	if p.match(TK_INTO) {
		// INTO requires a string literal filename or parameter placeholder
		if p.check(TK_STRING) {
			tok := p.advance()
			stmt.Into = Unquote(tok.Lexeme)
		} else if p.check(TK_VARIABLE) {
			// Parameter placeholder - mark it for parameter binding
			p.advance()
			stmt.IntoParam = true // Mark that the filename comes from a parameter
		} else {
			return nil, p.error("expected string literal or parameter after INTO")
		}
	}

	return stmt, nil
}
