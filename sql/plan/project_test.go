package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/src-d/go-mysql-server/mem"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func TestProject(t *testing.T) {
	require := require.New(t)
	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert(sql.NewRow("col1_1", "col2_1"))
	child.Insert(sql.NewRow("col1_2", "col2_2"))
	p := NewProject([]sql.Expression{expression.NewGetField(1, sql.Text, "col2", true)}, child)
	require.Equal(1, len(p.Children()))
	schema := sql.Schema{
		{Name: "col2", Type: sql.Text, Nullable: true},
	}
	require.Equal(schema, p.Schema())
	iter, err := p.RowIter()
	require.Nil(err)
	require.NotNil(iter)
	row, err := iter.Next()
	require.Nil(err)
	require.NotNil(row)
	require.Equal(1, len(row))
	require.Equal("col2_1", row[0])
	row, err = iter.Next()
	require.Nil(err)
	require.NotNil(row)
	require.Equal(1, len(row))
	require.Equal("col2_2", row[0])
	row, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(row)

	p = NewProject(nil, child)
	require.Equal(0, len(p.Schema()))

	p = NewProject([]sql.Expression{
		expression.NewAlias(
			expression.NewGetField(1, sql.Text, "col2", true),
			"foo",
		),
	}, child)
	schema = sql.Schema{
		{Name: "foo", Type: sql.Text, Nullable: true},
	}
	require.Equal(schema, p.Schema())
}
