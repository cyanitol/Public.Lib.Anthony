package schema

import (
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// View represents a database view definition.
type View struct {
	Name      string          // View name
	Columns   []string        // Optional explicit column names
	Select    *parser.SelectStmt // The SELECT statement defining the view
	SQL       string          // CREATE VIEW statement
	Temporary bool            // True for temporary views
}

// GetView retrieves a view by name.
// Returns the view and true if found, nil and false otherwise.
func (s *Schema) GetView(name string) (*View, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// SQLite view names are case-insensitive
	lowerName := strings.ToLower(name)
	for viewName, view := range s.Views {
		if strings.ToLower(viewName) == lowerName {
			return view, true
		}
	}
	return nil, false
}

// ListViews returns a sorted list of all view names.
func (s *Schema) ListViews() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var names []string
	for name := range s.Views {
		names = append(names, name)
	}
	// Use the same sorting approach as ListTables
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[i] > names[j] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

// CreateView creates a view from a CREATE VIEW statement.
func (s *Schema) CreateView(stmt *parser.CreateViewStmt) (*View, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for reserved names
	if IsReservedName(stmt.Name) {
		return nil, fmt.Errorf("view name is reserved: %s", stmt.Name)
	}

	// Check if view already exists
	if existing, err := s.checkViewExists(stmt.Name, stmt.IfNotExists); err != nil || existing != nil {
		return existing, err
	}

	// Create the view
	view := &View{
		Name:      stmt.Name,
		Columns:   stmt.Columns,
		Select:    stmt.Select,
		SQL:       generateCreateViewSQL(stmt),
		Temporary: stmt.Temporary,
	}

	s.Views[stmt.Name] = view
	return view, nil
}

// DropView removes a view from the schema.
func (s *Schema) DropView(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lowerName := strings.ToLower(name)

	// Find the actual view name (case-insensitive)
	for viewName := range s.Views {
		if strings.ToLower(viewName) == lowerName {
			delete(s.Views, viewName)
			return nil
		}
	}

	return fmt.Errorf("view not found: %s", name)
}

// checkViewExists checks if a view already exists in the schema.
// Returns the existing view if found and ifNotExists is true, otherwise an error.
func (s *Schema) checkViewExists(name string, ifNotExists bool) (*View, error) {
	lowerName := strings.ToLower(name)
	for viewName, view := range s.Views {
		if strings.ToLower(viewName) == lowerName {
			if ifNotExists {
				return view, nil
			}
			return nil, fmt.Errorf("view already exists: %s", name)
		}
	}
	return nil, nil
}

// generateCreateViewSQL generates the CREATE VIEW SQL text from the AST.
func generateCreateViewSQL(stmt *parser.CreateViewStmt) string {
	var sql strings.Builder
	sql.WriteString("CREATE ")
	if stmt.Temporary {
		sql.WriteString("TEMP ")
	}
	sql.WriteString("VIEW ")
	if stmt.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(stmt.Name)

	// Add column list if specified
	if len(stmt.Columns) > 0 {
		sql.WriteString("(")
		for i, col := range stmt.Columns {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(col)
		}
		sql.WriteString(")")
	}

	sql.WriteString(" AS ")
	if stmt.Select != nil {
		sql.WriteString(stmt.Select.String())
	}

	return sql.String()
}

// parseViewSQL parses a CREATE VIEW statement from a master row.
func (s *Schema) parseViewSQL(row MasterRow) (*View, error) {
	if row.SQL == "" {
		return &View{
			Name:    row.Name,
			SQL:     row.SQL,
			Columns: []string{},
		}, nil
	}

	// Parse the SQL statement
	p := parser.NewParser(row.SQL)
	stmts, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Should have exactly one statement
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected 1 statement, got %d", len(stmts))
	}

	// Ensure it's a CREATE VIEW statement
	createView, ok := stmts[0].(*parser.CreateViewStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE VIEW, got %T", stmts[0])
	}

	// Create the view
	view := &View{
		Name:      createView.Name,
		Columns:   createView.Columns,
		Select:    createView.Select,
		SQL:       row.SQL,
		Temporary: createView.Temporary,
	}

	return view, nil
}
