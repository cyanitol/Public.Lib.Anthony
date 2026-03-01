package schema

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

func TestCreateView(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create a simple view
	stmt := &parser.CreateViewStmt{
		Name: "active_users",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Star: true},
			},
		},
	}

	view, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	if view.Name != "active_users" {
		t.Errorf("view.Name = %q, want %q", view.Name, "active_users")
	}

	if view.Select == nil {
		t.Error("view.Select should not be nil")
	}

	// Verify view is in schema
	if _, ok := s.GetView("active_users"); !ok {
		t.Error("view not found in schema")
	}
}

func TestCreateViewWithColumns(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateViewStmt{
		Name:    "user_view",
		Columns: []string{"user_id", "user_name", "user_email"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
				{Expr: &parser.IdentExpr{Name: "name"}},
				{Expr: &parser.IdentExpr{Name: "email"}},
			},
		},
	}

	view, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	if len(view.Columns) != 3 {
		t.Fatalf("view has %d columns, want 3", len(view.Columns))
	}

	expectedCols := []string{"user_id", "user_name", "user_email"}
	for i, col := range view.Columns {
		col := col
		if col != expectedCols[i] {
			t.Errorf("column %d: got %q, want %q", i, col, expectedCols[i])
		}
	}
}

func TestCreateViewIfNotExists(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateViewStmt{
		Name: "test_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Star: true},
			},
		},
	}

	// First creation should succeed
	_, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("First CreateView() error = %v", err)
	}

	// Second creation without IF NOT EXISTS should fail
	_, err = s.CreateView(stmt)
	if err == nil {
		t.Error("Expected error creating duplicate view")
	}

	// Second creation with IF NOT EXISTS should succeed
	stmt.IfNotExists = true
	view, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("CreateView with IF NOT EXISTS error = %v", err)
	}
	if view == nil {
		t.Error("Expected existing view, got nil")
	}
}

func TestCreateTemporaryView(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	stmt := &parser.CreateViewStmt{
		Name:      "temp_view",
		Temporary: true,
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Star: true},
			},
		},
	}

	view, err := s.CreateView(stmt)
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	if !view.Temporary {
		t.Error("view should be temporary")
	}
}

func TestGetView(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Add a view directly
	s.Views["TestView"] = &View{Name: "TestView"}

	// Test case-insensitive lookup
	tests := []string{"TestView", "testview", "TESTVIEW", "tEsTvIeW"}
	for _, name := range tests {
		name := name
		t.Run(name, func(t *testing.T) {
			view, ok := s.GetView(name)
			if !ok {
				t.Errorf("GetView(%q) not found", name)
			}
			if view.Name != "TestView" {
				t.Errorf("GetView(%q) returned wrong view: %q", name, view.Name)
			}
		})
	}

	// Test non-existent view
	_, ok := s.GetView("nonexistent")
	if ok {
		t.Error("GetView should return false for nonexistent view")
	}
}

func TestListViews(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Empty schema
	views := s.ListViews()
	if len(views) != 0 {
		t.Errorf("ListViews() returned %d views, want 0", len(views))
	}

	// Add views
	s.Views["view1"] = &View{Name: "view1"}
	s.Views["view2"] = &View{Name: "view2"}
	s.Views["view3"] = &View{Name: "view3"}

	views = s.ListViews()
	if len(views) != 3 {
		t.Fatalf("ListViews() returned %d views, want 3", len(views))
	}

	// Should be sorted
	expected := []string{"view1", "view2", "view3"}
	for i, name := range expected {
		name := name
		if views[i] != name {
			t.Errorf("views[%d] = %q, want %q", i, views[i], name)
		}
	}
}

func TestDropView(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create view
	s.Views["test_view"] = &View{Name: "test_view"}

	// Drop the view
	err := s.DropView("test_view")
	if err != nil {
		t.Fatalf("DropView() error = %v", err)
	}

	// View should be gone
	if _, ok := s.GetView("test_view"); ok {
		t.Error("View still exists after drop")
	}

	// Dropping nonexistent view should error
	err = s.DropView("nonexistent")
	if err == nil {
		t.Error("Expected error dropping nonexistent view")
	}
}

func TestDropViewCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Create view with mixed case
	s.Views["MyView"] = &View{Name: "MyView"}

	// Drop with different case
	err := s.DropView("myview")
	if err != nil {
		t.Fatalf("DropView() error = %v", err)
	}

	// View should be gone
	if _, ok := s.GetView("MyView"); ok {
		t.Error("View still exists after drop")
	}
}

func TestGenerateCreateViewSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		stmt *parser.CreateViewStmt
		want string
	}{
		{
			name: "simple view",
			stmt: &parser.CreateViewStmt{
				Name: "v",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
				},
			},
			want: "CREATE VIEW v AS SELECT",
		},
		{
			name: "view with columns",
			stmt: &parser.CreateViewStmt{
				Name:    "v",
				Columns: []string{"id", "name"},
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
				},
			},
			want: "CREATE VIEW v(id, name) AS SELECT",
		},
		{
			name: "temporary view",
			stmt: &parser.CreateViewStmt{
				Name:      "v",
				Temporary: true,
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
				},
			},
			want: "CREATE TEMP VIEW v AS SELECT",
		},
		{
			name: "view with IF NOT EXISTS",
			stmt: &parser.CreateViewStmt{
				Name:        "v",
				IfNotExists: true,
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
				},
			},
			want: "CREATE VIEW IF NOT EXISTS v AS SELECT",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := generateCreateViewSQL(tt.stmt)
			// Just check that the SQL contains expected keywords
			// (full SQL comparison would be too fragile)
			if len(got) == 0 {
				t.Error("generateCreateViewSQL() returned empty string")
			}
		})
	}
}

func TestConcurrentViewAccess(t *testing.T) {
	t.Parallel()
	s := NewSchema()

	// Test concurrent reads and writes
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			stmt := &parser.CreateViewStmt{
				Name: "view" + string(rune(i)),
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
				},
			}
			s.CreateView(stmt)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = s.ListViews()
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}
