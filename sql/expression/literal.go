package expression

import "github.com/src-d/go-mysql-server/sql"

type Literal struct {
	value     interface{}
	fieldType sql.Type
	name      string
}

func NewLiteral(value interface{}, fieldType sql.Type) *Literal {
	return &Literal{
		value:     value,
		fieldType: fieldType,
		name:      "literal_" + fieldType.Type().String(),
	}
}

func (p Literal) Resolved() bool {
	return true
}

func (p Literal) IsNullable() bool {
	return p.value == nil
}

func (p Literal) Type() sql.Type {
	return p.fieldType
}

func (p Literal) Eval(row sql.Row) interface{} {
	return p.value
}

func (p Literal) Name() string {
	return p.name
}

func (p *Literal) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	n := *p
	return f(&n)
}
