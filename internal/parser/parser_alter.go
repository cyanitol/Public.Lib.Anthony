package parser

// =============================================================================
// ALTER TABLE
// =============================================================================

func (p *Parser) parseAlter() (Statement, error) {
	if !p.match(TK_TABLE) {
		return nil, p.error("expected TABLE after ALTER")
	}
	return p.parseAlterTable()
}

func (p *Parser) parseAlterTable() (*AlterTableStmt, error) {
	stmt := &AlterTableStmt{}

	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)

	action, err := p.parseAlterTableAction()
	if err != nil {
		return nil, err
	}
	stmt.Action = action

	return stmt, nil
}

func (p *Parser) parseAlterTableAction() (AlterTableAction, error) {
	if p.match(TK_RENAME) {
		return p.parseAlterTableRename()
	} else if p.match(TK_ADD) {
		return p.parseAlterTableAdd()
	} else if p.match(TK_DROP) {
		return p.parseAlterTableDrop()
	}
	return nil, p.error("expected RENAME, ADD, or DROP after table name")
}

// parseAlterTableRename handles RENAME TO newname and RENAME COLUMN oldname TO newname.
func (p *Parser) parseAlterTableRename() (AlterTableAction, error) {
	if p.match(TK_TO) {
		// RENAME TO newname
		if !p.check(TK_ID) {
			return nil, p.error("expected new table name after RENAME TO")
		}
		return &RenameTableAction{NewName: Unquote(p.advance().Lexeme)}, nil
	}

	if p.match(TK_COLUMN) {
		// RENAME COLUMN oldname TO newname
		if !p.check(TK_ID) {
			return nil, p.error("expected column name after RENAME COLUMN")
		}
		oldName := Unquote(p.advance().Lexeme)

		if !p.match(TK_TO) {
			return nil, p.error("expected TO after column name")
		}

		if !p.check(TK_ID) {
			return nil, p.error("expected new column name after TO")
		}
		newName := Unquote(p.advance().Lexeme)

		return &RenameColumnAction{OldName: oldName, NewName: newName}, nil
	}

	return nil, p.error("expected TO or COLUMN after RENAME")
}

// parseAlterTableAdd handles ADD COLUMN column_def.
func (p *Parser) parseAlterTableAdd() (AlterTableAction, error) {
	// COLUMN keyword is optional in SQLite
	p.match(TK_COLUMN)

	col, err := p.parseColumnDef()
	if err != nil {
		return nil, err
	}

	return &AddColumnAction{Column: *col}, nil
}

// parseAlterTableDrop handles DROP COLUMN column_name.
func (p *Parser) parseAlterTableDrop() (AlterTableAction, error) {
	if !p.match(TK_COLUMN) {
		return nil, p.error("expected COLUMN after DROP")
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected column name after DROP COLUMN")
	}
	columnName := Unquote(p.advance().Lexeme)

	return &DropColumnAction{ColumnName: columnName}, nil
}
