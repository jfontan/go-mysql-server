package analyzer_test

import (
	"fmt"
	"testing"

	"github.com/src-d/go-mysql-server/mem"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/plan"

	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Analyze(t *testing.T) {
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i2", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("mytable2", table2)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}
	a := analyzer.New(catalog)
	a.CurrentDatabase = "mydb"

	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := a.Analyze(notAnalyzed)
	assert.NoError(err)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = a.Analyze(notAnalyzed)
	assert.Error(err)
	assert.Equal(notAnalyzed, analyzed)

	analyzed, err = a.Analyze(table)
	assert.NoError(err)
	assert.Equal(table, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("o")},
		plan.NewUnresolvedTable("mytable"),
	)
	_, err = a.Analyze(notAnalyzed)
	assert.Error(err)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	var expected sql.Node = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		table,
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewDescribe(
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewDescribe(table)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		table,
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		plan.NewProject(
			[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
			table,
		),
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewUnresolvedColumn("i"),
				"foo",
			),
		},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewAlias(
				expression.NewGetField(0, sql.Int32, "i", false),
				"foo",
			),
		},
		table,
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{expression.NewUnresolvedColumn("i")},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral(int32(1), sql.Int32),
			),
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewGetField(0, sql.Int32, "i", false),
				expression.NewLiteral(int32(1), sql.Int32),
			),
			table,
		),
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
			expression.NewUnresolvedColumn("i2"),
		},
		plan.NewCrossJoin(
			plan.NewUnresolvedTable("mytable"),
			plan.NewUnresolvedTable("mytable2"),
		),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Int32, "i", false),
			expression.NewGetField(1, sql.Int32, "i2", false),
		},
		plan.NewCrossJoin(table, table2),
	)
	assert.NoError(err)
	assert.Equal(expected, analyzed)

	notAnalyzed = plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("i"),
			},
			plan.NewUnresolvedTable("mytable"),
		),
	)
	analyzed, err = a.Analyze(notAnalyzed)
	expected = plan.NewLimit(int64(1),
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetField(0, sql.Int32, "i", false),
			},
			table,
		),
	)
	assert.Nil(err)
	assert.Equal(expected, analyzed)
}

func TestAnalyzer_Analyze_MaxIterations(t *testing.T) {
	assert := require.New(t)

	catalog := &sql.Catalog{}
	a := analyzer.New(catalog)
	a.CurrentDatabase = "mydb"

	i := 0
	a.Rules = []analyzer.Rule{{
		"infinite",
		func(a *analyzer.Analyzer, n sql.Node) sql.Node {
			i += 1
			return plan.NewUnresolvedTable(fmt.Sprintf("table%d", i))
		},
	}}

	notAnalyzed := plan.NewUnresolvedTable("mytable")
	analyzed, err := a.Analyze(notAnalyzed)
	assert.NotNil(err)
	assert.Equal(plan.NewUnresolvedTable("table1001"), analyzed)
}
