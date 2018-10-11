package plan

import (
	"fmt"
	"strings"
)

import (
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var ErrTableNotFound = errors.NewKind("Table `%s` not found")

// ShowCreateTable is a node that shows the CREATE TABLE statement for a table.
type ShowCreateTable struct {
	DatabaseName string
	Table        string
	Registry     *sql.IndexRegistry
	Catalog      *sql.Catalog
}

// Schema implements the Node interface.
func (n *ShowCreateTable) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Table", Type: sql.Text, Nullable: false},
		&sql.Column{Name: "Create Table", Type: sql.Text, Nullable: false},
	}
}

// TransformExpressionsUp implements the Transformable interface.
func (n *ShowCreateTable) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return n, nil
}

// TransformUp implements the Transformable interface.
func (n *ShowCreateTable) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	var datab sql.Database
	if n.Catalog == nil {
		datab = &sql.UnresolvedDatabase{}
		return f(NewShowCreateTable(n.DatabaseName, n.Table, n.Registry))
	}
	datab, _ = n.Catalog.Database(n.DatabaseName)
	if datab == nil {
		datab = &sql.UnresolvedDatabase{}
	}

	return f(NewShowCreateTable(n.DatabaseName, n.Table, n.Registry))
}

// RowIter implements the Node interface.
func (n *ShowCreateTable) RowIter(*sql.Context) (sql.RowIter, error) {
	var datab sql.Database
	if n.Catalog == nil {
		datab = &sql.UnresolvedDatabase{}

		return &showCreateTablesIter{
			db:    datab,
			table: n.Table,
		}, nil
	}

	datab, _ = n.Catalog.Database(n.DatabaseName)
	if datab == nil {
		datab = &sql.UnresolvedDatabase{}
	}

	return &showCreateTablesIter{
		db:    datab,
		table: n.Table,
	}, nil
}

// String implements the Stringer interface.
func (n *ShowCreateTable) String() string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", n.Table)
}

type createTableStmt struct {
	colName string
	colType sql.Type
}

type showCreateTablesIter struct {
	db    sql.Database
	table string

	createStmt *createTableStmt
}

func (i *showCreateTablesIter) Next() (sql.Row, error) {
	table := i.db.Tables()[i.table]

	if table == nil {
		return nil, ErrTableNotFound.New(table)
	}

	schema := table.Schema()
	colCreateStatements := make([]string, len(schema), len(schema))
	// Statement creation parts for each column
	for indx, col := range schema {
		createStmtPart := fmt.Sprintf("`%s` %s", col.Name, col.Type.Type())
		if col.Default != nil {
			createStmtPart = fmt.Sprintf("%s DEFAULT %v", createStmtPart, col.Default)
		}

		if !col.Nullable {
			createStmtPart = fmt.Sprintf("%sNOT NULL", createStmtPart)
		}

		colCreateStatements[indx] = createStmtPart
	}

	prettyColCreateStmts := fmt.Sprintf("%s", stripBrackets(strings.Join(colCreateStatements, ",\n")))

	composedCreateTableStatement :=
		fmt.Sprintf("CREATE TABLE `%s` (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", i.table, prettyColCreateStmts)

	return sql.NewRow(
		i.table,                      // "Table" string
		composedCreateTableStatement, // "Create Table" string
	), nil
}

func stripBrackets(val interface{}) string {
	return strings.Trim(fmt.Sprintf("%s", val), "[]")
}

func (i *showCreateTablesIter) Close() error {
	return nil
}

// NewShowCreateTable creates a new ShowCreateTable node.
func NewShowCreateTable(db string, table string, registry *sql.IndexRegistry) sql.Node {
	return &ShowCreateTable{DatabaseName: db,
		Table:    table,
		Registry: registry}
}

// Resolved implements the Resolvable interface.
func (n *ShowCreateTable) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (n *ShowCreateTable) Children() []sql.Node { return nil }
