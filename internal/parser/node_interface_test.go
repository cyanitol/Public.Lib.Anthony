package parser

import (
	"testing"
)

// Test that node(), statement(), and expression() interface methods work correctly
// These are interface satisfaction tests to ensure coverage

func TestNodeInterface(t *testing.T) {
	// Test all statements implement Node and Statement
	var _ Node = &SelectStmt{}
	var _ Node = &InsertStmt{}
	var _ Node = &UpdateStmt{}
	var _ Node = &DeleteStmt{}
	var _ Node = &CreateTableStmt{}
	var _ Node = &DropTableStmt{}
	var _ Node = &CreateIndexStmt{}
	var _ Node = &DropIndexStmt{}
	var _ Node = &CreateViewStmt{}
	var _ Node = &DropViewStmt{}
	var _ Node = &CreateTriggerStmt{}
	var _ Node = &DropTriggerStmt{}
	var _ Node = &BeginStmt{}
	var _ Node = &CommitStmt{}
	var _ Node = &RollbackStmt{}
	var _ Node = &ExplainStmt{}
	var _ Node = &AttachStmt{}
	var _ Node = &DetachStmt{}
	var _ Node = &PragmaStmt{}
	var _ Node = &AlterTableStmt{}
	var _ Node = &VacuumStmt{}

	// Test all statements implement Statement
	var _ Statement = &SelectStmt{}
	var _ Statement = &InsertStmt{}
	var _ Statement = &UpdateStmt{}
	var _ Statement = &DeleteStmt{}
	var _ Statement = &CreateTableStmt{}
	var _ Statement = &DropTableStmt{}
	var _ Statement = &CreateIndexStmt{}
	var _ Statement = &DropIndexStmt{}
	var _ Statement = &CreateViewStmt{}
	var _ Statement = &DropViewStmt{}
	var _ Statement = &CreateTriggerStmt{}
	var _ Statement = &DropTriggerStmt{}
	var _ Statement = &BeginStmt{}
	var _ Statement = &CommitStmt{}
	var _ Statement = &RollbackStmt{}
	var _ Statement = &ExplainStmt{}
	var _ Statement = &AttachStmt{}
	var _ Statement = &DetachStmt{}
	var _ Statement = &PragmaStmt{}
	var _ Statement = &AlterTableStmt{}
	var _ Statement = &VacuumStmt{}

	// Call methods through interface to ensure coverage
	statements := []Statement{
		&SelectStmt{},
		&InsertStmt{},
		&UpdateStmt{},
		&DeleteStmt{},
		&CreateTableStmt{},
		&DropTableStmt{},
		&CreateIndexStmt{},
		&DropIndexStmt{},
		&CreateViewStmt{},
		&DropViewStmt{},
		&CreateTriggerStmt{},
		&DropTriggerStmt{},
		&BeginStmt{},
		&CommitStmt{},
		&RollbackStmt{},
		&ExplainStmt{},
		&AttachStmt{},
		&DetachStmt{},
		&PragmaStmt{},
		&AlterTableStmt{},
		&VacuumStmt{},
	}

	for _, stmt := range statements {
		stmt.node()
		stmt.statement()
		_ = stmt.String()
	}
}

func TestExpressionInterface(t *testing.T) {
	// Test all expressions implement Node and Expression
	var _ Node = &BinaryExpr{}
	var _ Node = &UnaryExpr{}
	var _ Node = &LiteralExpr{}
	var _ Node = &IdentExpr{}
	var _ Node = &FunctionExpr{}
	var _ Node = &CaseExpr{}
	var _ Node = &InExpr{}
	var _ Node = &BetweenExpr{}
	var _ Node = &CastExpr{}
	var _ Node = &CollateExpr{}
	var _ Node = &ParenExpr{}
	var _ Node = &SubqueryExpr{}
	var _ Node = &ExistsExpr{}
	var _ Node = &VariableExpr{}

	// Test all expressions implement Expression
	var _ Expression = &BinaryExpr{}
	var _ Expression = &UnaryExpr{}
	var _ Expression = &LiteralExpr{}
	var _ Expression = &IdentExpr{}
	var _ Expression = &FunctionExpr{}
	var _ Expression = &CaseExpr{}
	var _ Expression = &InExpr{}
	var _ Expression = &BetweenExpr{}
	var _ Expression = &CastExpr{}
	var _ Expression = &CollateExpr{}
	var _ Expression = &ParenExpr{}
	var _ Expression = &SubqueryExpr{}
	var _ Expression = &ExistsExpr{}
	var _ Expression = &VariableExpr{}

	// Call methods through interface to ensure coverage
	expressions := []Expression{
		&BinaryExpr{},
		&UnaryExpr{},
		&LiteralExpr{Type: LiteralInteger, Value: "42"},
		&IdentExpr{Name: "x"},
		&FunctionExpr{Name: "COUNT"},
		&CaseExpr{},
		&InExpr{},
		&BetweenExpr{},
		&CastExpr{},
		&CollateExpr{},
		&ParenExpr{},
		&SubqueryExpr{},
		&ExistsExpr{},
		&VariableExpr{},
	}

	for _, expr := range expressions {
		expr.node()
		expr.expression()
		_ = expr.String()
	}
}

func TestAlterTableActionInterface(t *testing.T) {
	// Test all alter table actions implement Node and AlterTableAction
	var _ Node = &RenameTableAction{}
	var _ Node = &RenameColumnAction{}
	var _ Node = &AddColumnAction{}
	var _ Node = &DropColumnAction{}

	var _ AlterTableAction = &RenameTableAction{}
	var _ AlterTableAction = &RenameColumnAction{}
	var _ AlterTableAction = &AddColumnAction{}
	var _ AlterTableAction = &DropColumnAction{}

	// Call methods through interface to ensure coverage
	actions := []AlterTableAction{
		&RenameTableAction{NewName: "new_table"},
		&RenameColumnAction{OldName: "old_col", NewName: "new_col"},
		&AddColumnAction{Column: ColumnDef{Name: "col", Type: "TEXT"}},
		&DropColumnAction{ColumnName: "col"},
	}

	for _, action := range actions {
		action.node()
		action.alterTableAction()
		_ = action.String()
	}
}
