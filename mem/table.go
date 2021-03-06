package mem

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	data   []sql.Row
}

func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name:   name,
		schema: schema,
	}
}

func (Table) Resolved() bool {
	return true
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) Children() []sql.Node {
	return []sql.Node{}
}

func (t *Table) RowIter() (sql.RowIter, error) {
	return sql.RowsToRowIter(t.data...), nil
}

func (t *Table) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(t)
}

func (t *Table) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}

func (t *Table) Insert(row sql.Row) error {
	if len(row) != len(t.schema) {
		return fmt.Errorf("insert expected %d values, got %d", len(t.schema), len(row))
	}

	for idx, value := range row {
		c := t.schema[idx]
		if !c.Check(value) {
			return sql.ErrInvalidType
		}
	}

	t.data = append(t.data, row.Copy())
	return nil
}
