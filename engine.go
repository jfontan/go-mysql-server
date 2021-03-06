package sqle

import (
	"errors"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/parse"
)

var (
	ErrNotSupported = errors.New("feature not supported yet")
)

// Engine is a SQL engine.
// It implements the standard database/sql/driver/Driver interface, so it can
// be registered as a database/sql driver.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
}

// New creates a new Engine.
func New() *Engine {
	c := sql.NewCatalog()
	err := expression.RegisterDefaults(c)
	if err != nil {
		panic(err)
	}

	a := analyzer.New(c)
	return &Engine{c, a}
}

// Query executes a query without attaching to any session.
func (e *Engine) Query(query string) (sql.Schema, sql.RowIter, error) {
	parsed, err := parse.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err := analyzed.RowIter()
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.Databases = append(e.Catalog.Databases, db)
	e.Analyzer.CurrentDatabase = db.Name()
}
