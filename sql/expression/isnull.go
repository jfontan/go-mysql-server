package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// IsNull is an expression that checks if an expression is null.
type IsNull struct {
	UnaryExpression
}

// NewIsNull creates a new IsNull expression.
func NewIsNull(child sql.Expression) *IsNull {
	return &IsNull{UnaryExpression{child}}
}

// Type implements the Expression interface.
func (e *IsNull) Type() sql.Type {
	return sql.Boolean
}

// IsNullable implements the Expression interface.
func (e *IsNull) IsNullable() bool {
	return false
}

// Eval implements the Expression interface.
func (e *IsNull) Eval(session sql.Session, row sql.Row) (interface{}, error) {
	v, err := e.Child.Eval(session, row)
	if err != nil {
		return nil, err
	}

	return v == nil, nil
}

// Name implements the Expression interface.
func (e *IsNull) Name() string {
	return "IsNull(" + e.Child.Name() + ")"
}

// TransformUp implements the Expression interface.
func (e *IsNull) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewIsNull(e.Child.TransformUp(f)))
}
