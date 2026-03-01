// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// Test that node(), statement(), and expression() interface methods work correctly
// These are interface satisfaction tests to ensure coverage

func TestNodeInterface(t *testing.T) {
	t.Parallel()
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

	// Call methods directly on concrete types to ensure coverage
	(&SelectStmt{}).node()
	(&SelectStmt{}).statement()
	(&InsertStmt{}).node()
	(&InsertStmt{}).statement()
	(&UpdateStmt{}).node()
	(&UpdateStmt{}).statement()
	(&DeleteStmt{}).node()
	(&DeleteStmt{}).statement()
	(&CreateTableStmt{}).node()
	(&CreateTableStmt{}).statement()
	(&DropTableStmt{}).node()
	(&DropTableStmt{}).statement()
	(&CreateIndexStmt{}).node()
	(&CreateIndexStmt{}).statement()
	(&DropIndexStmt{}).node()
	(&DropIndexStmt{}).statement()
	(&CreateViewStmt{}).node()
	(&CreateViewStmt{}).statement()
	(&DropViewStmt{}).node()
	(&DropViewStmt{}).statement()
	(&CreateTriggerStmt{}).node()
	(&CreateTriggerStmt{}).statement()
	(&DropTriggerStmt{}).node()
	(&DropTriggerStmt{}).statement()
	(&BeginStmt{}).node()
	(&BeginStmt{}).statement()
	(&CommitStmt{}).node()
	(&CommitStmt{}).statement()
	(&RollbackStmt{}).node()
	(&RollbackStmt{}).statement()
	(&ExplainStmt{}).node()
	(&ExplainStmt{}).statement()
	(&AttachStmt{}).node()
	(&AttachStmt{}).statement()
	(&DetachStmt{}).node()
	(&DetachStmt{}).statement()
	(&PragmaStmt{}).node()
	(&PragmaStmt{}).statement()
	(&AlterTableStmt{}).node()
	(&AlterTableStmt{}).statement()
	(&VacuumStmt{}).node()
	(&VacuumStmt{}).statement()
}

func TestExpressionInterface(t *testing.T) {
	t.Parallel()
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

	// Call methods directly on concrete types to ensure coverage
	(&BinaryExpr{}).node()
	(&BinaryExpr{}).expression()
	(&UnaryExpr{}).node()
	(&UnaryExpr{}).expression()
	(&LiteralExpr{}).node()
	(&LiteralExpr{}).expression()
	(&IdentExpr{}).node()
	(&IdentExpr{}).expression()
	(&FunctionExpr{}).node()
	(&FunctionExpr{}).expression()
	(&CaseExpr{}).node()
	(&CaseExpr{}).expression()
	(&InExpr{}).node()
	(&InExpr{}).expression()
	(&BetweenExpr{}).node()
	(&BetweenExpr{}).expression()
	(&CastExpr{}).node()
	(&CastExpr{}).expression()
	(&CollateExpr{}).node()
	(&CollateExpr{}).expression()
	(&ParenExpr{}).node()
	(&ParenExpr{}).expression()
	(&SubqueryExpr{}).node()
	(&SubqueryExpr{}).expression()
	(&ExistsExpr{}).node()
	(&ExistsExpr{}).expression()
	(&VariableExpr{}).node()
	(&VariableExpr{}).expression()
}

func TestAlterTableActionInterface(t *testing.T) {
	t.Parallel()
	// Test all alter table actions implement Node and AlterTableAction
	var _ Node = &RenameTableAction{}
	var _ Node = &RenameColumnAction{}
	var _ Node = &AddColumnAction{}
	var _ Node = &DropColumnAction{}

	var _ AlterTableAction = &RenameTableAction{}
	var _ AlterTableAction = &RenameColumnAction{}
	var _ AlterTableAction = &AddColumnAction{}
	var _ AlterTableAction = &DropColumnAction{}

	// Call methods directly on concrete types to ensure coverage
	(&RenameTableAction{}).node()
	(&RenameTableAction{}).alterTableAction()
	(&RenameColumnAction{}).node()
	(&RenameColumnAction{}).alterTableAction()
	(&AddColumnAction{}).node()
	(&AddColumnAction{}).alterTableAction()
	(&DropColumnAction{}).node()
	(&DropColumnAction{}).alterTableAction()
}
