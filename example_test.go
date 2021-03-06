package sqle_test

import (
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/mem"
	gitqlsql "github.com/src-d/go-mysql-server/sql"
)

func Example() {
	e := sqle.New()
	// Create a test memory database and register it to the default engine.
	e.AddDatabase(createTestDatabase())

	_, r, err := e.Query(`SELECT name, count(*) FROM mytable
	WHERE name = 'John Doe'
	GROUP BY name`)
	checkIfError(err)

	// Iterate results and print them.
	for {
		ro, err := r.Next()
		if err == io.EOF {
			break
		}
		checkIfError(err)

		name := ro[0]
		count := ro[1]

		fmt.Println(name, count)
	}

	// Output: John Doe 2
}

func checkIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func createTestDatabase() *mem.Database {
	db := mem.NewDatabase("test")
	table := mem.NewTable("mytable", gitqlsql.Schema{
		{Name: "name", Type: gitqlsql.Text},
		{Name: "email", Type: gitqlsql.Text},
	})
	db.AddTable("mytable", table)
	table.Insert(gitqlsql.NewRow("John Doe", "john@doe.com"))
	table.Insert(gitqlsql.NewRow("John Doe", "johnalt@doe.com"))
	table.Insert(gitqlsql.NewRow("Jane Doe", "jane@doe.com"))
	table.Insert(gitqlsql.NewRow("Evil Bob", "evilbob@gmail.com"))
	return db
}
