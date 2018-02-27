package plan

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Noop is a node that returns Query OK
type Noop struct{}

// NewNoop creates an Noop node.
func NewNoop() *Noop {
	return &Noop{}
}

// Schema implements the Node interface.
func (p *Noop) Schema() sql.Schema {
	return nil
}

// Execute returns 0
func (p *Noop) Execute() (int, error) {
	return 0, nil
}

// RowIter implements the Node interface.
func (p *Noop) RowIter() (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// TransformUp implements the Transformable interface.
func (p *Noop) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return p
}

// TransformExpressionsUp implements the Transformable interface.
func (p *Noop) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return NewNoop()
}

// Children implements the Node interface.
func (p *Noop) Children() []sql.Node {
	return nil
}

// Resolved implements the Resolvable interface.
func (p *Noop) Resolved() bool {
	return true
}
