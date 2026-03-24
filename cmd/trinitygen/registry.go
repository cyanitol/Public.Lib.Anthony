// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

package main

import "fmt"

// moduleRegistry returns all known module specifications.
// Each module corresponds to one trinity_<module>_test.go file.
func moduleRegistry() []ModuleSpec {
	return []ModuleSpec{
		coreModule(),
		typesModule(),
		nullModule(),
		boundaryModule(),
		selectModule(),
		insertModule(),
		updateModule(),
		ddlModule(),
		joinModule(),
		compoundModule(),
		collationModule(),
		pragmaModule(),
		windowModule(),
		jsonModule(),
		vtabModule(),
		indexModule(),
		exprModule(),
		funcModule(),
		transModule(),
		fkeyModule(),
		triggerModule(),
		viewModule(),
		cteModule(),
	}
}

func coreModule() ModuleSpec {
	return ModuleSpec{
		Module: "core", TestFunc: "TestTrinity_Core", BuildFunc: "buildTrinityCoreTests",
		Comment:  "TestTrinity_Core exercises CREATE TABLE, INSERT/SELECT/UPDATE/DELETE, ROWID, IPK.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genCoreCreateTableTests"},
			{Name: "genCoreInsertSelectTests"},
			{Name: "genCoreUpdateDeleteTests"},
			{Name: "genCoreWhereTests"},
			{Name: "genCoreLimitOffsetTests"},
			{Name: "genCoreRowidTests"},
			{Name: "genCoreIPKTests"},
			{Name: "genCoreDDLTests"},
		},
	}
}

func typesModule() ModuleSpec {
	return ModuleSpec{
		Module: "types", TestFunc: "TestTrinity_Types", BuildFunc: "buildTrinityTypesTests",
		Comment:  "TestTrinity_Types exercises type affinity, TYPEOF, CAST, implicit coercion.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genTypeAffinityMatrixTests"},
			{Name: "genTypeofTests"},
			{Name: "genTypeCastTests"},
			{Name: "genImplicitCoercionTests"},
		},
	}
}

func nullModule() ModuleSpec {
	return ModuleSpec{
		Module: "null", TestFunc: "TestTrinity_Null", BuildFunc: "buildTrinityNullTests",
		Comment:  "TestTrinity_Null exercises NULL propagation, IS NULL, COALESCE, IFNULL, NULLIF.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genNullArithPropTests"},
			{Name: "genNullComparePropTests"},
			{Name: "genNullIsNullTests"},
			{Name: "genNullCoalesceIfnullTests"},
			{Name: "genNullNullifTests"},
			{Name: "genNullCaseWhenTests"},
			{Name: "genNullAggregateTests"},
			{Name: "genNullGroupByOrderByTests"},
			{Name: "genNullDistinctInBetweenTests"},
		},
	}
}

func boundaryModule() ModuleSpec {
	return ModuleSpec{
		Module: "boundary", TestFunc: "TestTrinity_Boundary", BuildFunc: "buildTrinityBoundaryTests",
		Comment:     "TestTrinity_Boundary exercises INT64/INT32 edges, float edges, string edges.",
		NeedsFmt:    true,
		NeedsMath:   true,
		NeedsString: true,
		SubFuncs: []SubFunc{
			{Name: "genBoundaryIntTests"},
			{Name: "genBoundaryArithEdgeTests"},
			{Name: "genBoundarySmallValArithTests"},
			{Name: "genBoundaryFloatTests"},
			{Name: "genBoundaryStringTests"},
			{Name: "genBoundaryLimitTests"},
			{Name: "genBoundarySQLLimitTests"},
		},
	}
}

func selectModule() ModuleSpec {
	return ModuleSpec{
		Module: "select", TestFunc: "TestTrinity_Select", BuildFunc: "buildTrinitySelectTests",
		Comment:  "TestTrinity_Select exercises SELECT forms, WHERE, ORDER BY, GROUP BY, subqueries.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genSelectBasicTests"},
			{Name: "genSelectWhereTests"},
			{Name: "genSelectOrderByTests"},
			{Name: "genSelectLimitOffsetTests"},
			{Name: "genSelectDistinctTests"},
			{Name: "genSelectGroupByHavingTests"},
			{Name: "genSelectSubqueryTests"},
		},
	}
}

func insertModule() ModuleSpec {
	return ModuleSpec{
		Module: "insert", TestFunc: "TestTrinity_Insert", BuildFunc: "buildTrinityInsertTests",
		Comment:  "TestTrinity_Insert exercises INSERT variants, conflicts, AUTOINCREMENT.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genInsertBasicTests"},
			{Name: "genInsertMultiRowTests"},
			{Name: "genInsertSelectTests"},
			{Name: "genInsertConflictTests"},
			{Name: "genInsertAutoincTests"},
			{Name: "genInsertErrorTests"},
		},
	}
}

func updateModule() ModuleSpec {
	return ModuleSpec{
		Module: "update", TestFunc: "TestTrinity_Update", BuildFunc: "buildTrinityUpdateTests",
		Comment:  "TestTrinity_Update exercises UPDATE/DELETE with WHERE, expressions, errors.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genUpdateBasicTests"},
			{Name: "genUpdateExprTests"},
			{Name: "genDeleteTests"},
			{Name: "genUpdateErrorTests"},
		},
	}
}

func ddlModule() ModuleSpec {
	return ModuleSpec{
		Module: "ddl", TestFunc: "TestTrinity_DDL", BuildFunc: "buildTrinityDDLTests",
		Comment:  "TestTrinity_DDL exercises CREATE/ALTER/DROP TABLE, INDEX, VIEW, REINDEX.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genDDLCreateTableTests"},
			{Name: "genDDLConstraintTests"},
			{Name: "genDDLCompositePKTests"},
			{Name: "genDDLIfNotExistsTests"},
			{Name: "genDDLAlterTableTests"},
			{Name: "genDDLDropTableTests"},
			{Name: "genDDLIndexTests"},
			{Name: "genDDLViewTests"},
			{Name: "genDDLReindexTests"},
		},
	}
}

func joinModule() ModuleSpec {
	return ModuleSpec{
		Module: "join", TestFunc: "TestTrinity_Join", BuildFunc: "buildTrinityJoinTests",
		Comment: "TestTrinity_Join exercises INNER/LEFT/CROSS JOIN, self-join, NULL keys, NATURAL, USING.",
		SubFuncs: []SubFunc{
			{Name: "genJoinInnerTests"},
			{Name: "genJoinLeftTests"},
			{Name: "genJoinCrossTests"},
			{Name: "genJoinSelfTests"},
			{Name: "genJoinNullKeyTests"},
			{Name: "genJoinNaturalUsingTests"},
			{Name: "genJoinMultiTableTests"},
		},
	}
}

func compoundModule() ModuleSpec {
	return ModuleSpec{
		Module: "compound", TestFunc: "TestTrinity_Compound", BuildFunc: "buildTrinityCompoundTests",
		Comment: "TestTrinity_Compound exercises UNION/UNION ALL/INTERSECT/EXCEPT.",
		SubFuncs: []SubFunc{
			{Name: "genCompoundUnionTests"},
			{Name: "genCompoundIntersectExceptTests"},
			{Name: "genCompoundOrderLimitTests"},
			{Name: "genCompoundNestedTests"},
		},
	}
}

func collationModule() ModuleSpec {
	return ModuleSpec{
		Module: "collation", TestFunc: "TestTrinity_Collation", BuildFunc: "buildTrinityCollationTests",
		Comment:  "TestTrinity_Collation exercises BINARY/NOCASE/RTRIM collation sequences.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genCollationBinaryTests"},
			{Name: "genCollationNocaseTests"},
			{Name: "genCollationRtrimTests"},
			{Name: "genCollationIndexTests"},
		},
	}
}

func pragmaModule() ModuleSpec {
	return ModuleSpec{
		Module: "pragma", TestFunc: "TestTrinity_Pragma", BuildFunc: "buildTrinityPragmaTests",
		Comment: "TestTrinity_Pragma exercises table_info, index_list, database_list, etc.",
		SubFuncs: []SubFunc{
			{Name: "genPragmaTableInfoTests"},
			{Name: "genPragmaIndexListTests"},
			{Name: "genPragmaDatabaseListTests"},
			{Name: "genPragmaForeignKeysTests"},
			{Name: "genPragmaCacheSizeTests"},
		},
	}
}

func windowModule() ModuleSpec {
	return ModuleSpec{
		Module: "window", TestFunc: "TestTrinity_Window", BuildFunc: "buildTrinityWindowTests",
		Comment: "TestTrinity_Window exercises ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, etc.",
		SubFuncs: []SubFunc{
			{Name: "genWindowRankTests"},
			{Name: "genWindowValueTests"},
			{Name: "genWindowFrameTests"},
			{Name: "genWindowAggregateTests"},
		},
	}
}

func jsonModule() ModuleSpec {
	return ModuleSpec{
		Module: "json", TestFunc: "TestTrinity_JSON", BuildFunc: "buildTrinityJSONTests",
		Comment: "TestTrinity_JSON exercises json/json_array/json_object/json_extract/json_type/json_valid.",
		SubFuncs: []SubFunc{
			{Name: "genJSONScalarTests"},
			{Name: "genJSONExtractTests"},
			{Name: "genJSONTypeValidTests"},
			{Name: "genJSONModifyTests"},
			{Name: "genJSONTableTests"},
		},
	}
}

func vtabModule() ModuleSpec {
	return ModuleSpec{
		Module: "vtab", TestFunc: "TestTrinity_VTab", BuildFunc: "buildTrinityVTabTests",
		Comment: "TestTrinity_VTab exercises json_each/json_tree as TVF in FROM clause.",
		SubFuncs: []SubFunc{
			{Name: "genVTabJSONEachTests"},
			{Name: "genVTabJSONTreeTests"},
			{Name: "genVTabJoinTests"},
		},
	}
}

func indexModule() ModuleSpec {
	return ModuleSpec{
		Module: "index", TestFunc: "TestTrinity_Index", BuildFunc: "buildTrinityIndexTests",
		Comment: "TestTrinity_Index exercises CREATE INDEX basic/unique/composite/partial, DROP, REINDEX.",
		SubFuncs: []SubFunc{
			{Name: "genIndexBasicTests"},
			{Name: "genIndexUniqueTests"},
			{Name: "genIndexCompositeTests"},
			{Name: "genIndexPartialTests"},
			{Name: "genIndexDropTests"},
			{Name: "genIndexVerifyTests"},
		},
	}
}

func exprModule() ModuleSpec {
	return ModuleSpec{
		Module: "expr", TestFunc: "TestTrinity_Expr", BuildFunc: "buildTrinityExprTests",
		Comment:  "TestTrinity_Expr exercises expression operators.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "genBinaryArithTests"},
			{Name: "genBinaryConcatTests"},
			{Name: "genBinaryCompareTests"},
			{Name: "genBinaryLogicTests"},
			{Name: "genBinaryISTests"},
			{Name: "genUnaryTests"},
			{Name: "genSpecialExprTests"},
		},
	}
}

func funcModule() ModuleSpec {
	return ModuleSpec{
		Module: "func", TestFunc: "TestTrinity_Func", BuildFunc: "buildTrinityFuncTests",
		Comment:  "TestTrinity_Func exercises built-in scalar, aggregate, and date/time functions.",
		NeedsFmt: true,
		SubFuncs: []SubFunc{
			{Name: "buildStringFuncTests"},
			{Name: "buildMathFuncTests"},
			{Name: "buildTypeFuncTests"},
			{Name: "buildDateTimeFuncTests"},
			{Name: "buildAggregateFuncTests"},
		},
	}
}

func transModule() ModuleSpec {
	return ModuleSpec{
		Module: "trans", TestFunc: "TestTrinity_Trans", BuildFunc: "buildTrinityTransTests",
		Comment: "TestTrinity_Trans exercises transaction BEGIN/COMMIT/ROLLBACK, SAVEPOINT.",
	}
}

func fkeyModule() ModuleSpec {
	return ModuleSpec{
		Module: "fkey", TestFunc: "TestTrinity_FKey", BuildFunc: "buildTrinityFKeyTests",
		Comment: "TestTrinity_FKey exercises foreign key constraints.",
	}
}

func triggerModule() ModuleSpec {
	return ModuleSpec{
		Module: "trigger", TestFunc: "TestTrinity_Trigger", BuildFunc: "buildTrinityTriggerTests",
		Comment: "TestTrinity_Trigger exercises CREATE/DROP TRIGGER.",
	}
}

func viewModule() ModuleSpec {
	return ModuleSpec{
		Module: "view", TestFunc: "TestTrinity_View", BuildFunc: "buildTrinityViewTests",
		Comment: "TestTrinity_View exercises CREATE/DROP VIEW.",
	}
}

func cteModule() ModuleSpec {
	return ModuleSpec{
		Module: "cte", TestFunc: "TestTrinity_CTE", BuildFunc: "buildTrinityCTETests",
		Comment: "TestTrinity_CTE exercises WITH common table expressions.",
	}
}

// lookupModule finds a module by name, or nil if not found.
func lookupModule(name string) *ModuleSpec {
	for _, m := range moduleRegistry() {
		if m.Module == name {
			return &m
		}
	}
	return nil
}

// moduleNames returns all registered module names.
func moduleNames() []string {
	reg := moduleRegistry()
	names := make([]string, len(reg))
	for i, m := range reg {
		names[i] = m.Module
	}
	return names
}

// reportModules prints a summary of all modules.
func reportModules() {
	for _, m := range moduleRegistry() {
		subs := len(m.SubFuncs)
		tests := len(m.Tests)
		fmt.Printf("%-12s %s (%d sub-generators, %d inline tests)\n",
			m.Module, m.TestFunc, subs, tests)
	}
}
