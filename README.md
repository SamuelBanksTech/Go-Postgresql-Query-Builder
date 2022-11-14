# Postgresql Query Builder for Go

[![GoReportCard](https://goreportcard.com/badge/github.com/SamuelBanksTech/Go-Postgresql-Query-Builder)](https://goreportcard.com/report/github.com/SamuelBanksTech/Go-Postgresql-Query-Builder)
[![made-with-Go](https://img.shields.io/badge/Made%20with-Go-1f425f.svg)](https://go.dev/)
[![GoDoc reference example](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/SamuelBanksTech/Go-Postgresql-Query-Builder/pqb)

This query builder aims to make complex queries for postgres easier to breakdown, put together, and read. The project is very much in its infancy, but is also in a very usable and functional state. 

However, please feel free to test, fork, or submit PRs. 

:-)

## Install

`go get github.com/SamuelBanksTech/Go-Postgresql-Query-Builder`

## Usage

Every example of usage would be unrealistic to show in this readme, but once you become familiar, it becomes quite intuitive. 

This query builder is best used with [`pgx by jackc`](https://github.com/jackc/pgx) but realistically this can be used with any postgres connection.

#### Basic Example
```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/SamuelBanksTech/Go-Postgresql-Query-Builder/pqb"
	"github.com/jackc/pgx/v4"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var qb pqb.Sqlbuilder

	pgQuery, queryArgs := qb.
		From(`myschema.widgets`).
		Select(`name`, `weight`).
		Where(`id`, `=`, `1`).
		Build()

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), pgQuery, queryArgs...).Scan(&name, &weight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(name, weight)
}
```
pgQuery Output:

`SELECT "name", "weight" FROM "myschema"."widgets" WHERE "id" = $1`

queryArgs Output:

[1]



#### Slightly More Advanced Example
```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/SamuelBanksTech/Go-Postgresql-Query-Builder/pqb"
	"github.com/jackc/pgx/v4"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var qb pqb.Sqlbuilder

	pgQuery, queryArgs := qb.
		From(`myschema.tasks`).
		LeftJoin(`myschema.users`, `users`, `myschema.tasks.user_id = users.id`).
		Where(`users.active`, `=`, `1`).
		Where(`myschema.tasks.completed`, `=`, `0`).
		Select(`myschema.tasks.task_details`, `users.name`, `users.email`).
		Build()


	rows, _ := conn.Query(context.Background(), pgQuery, queryArgs...)

	for rows.Next() {
		var taskData string
		var userName string
		var userEmail string
		
		err := rows.Scan(&taskData, &userName, &userEmail)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s - %s - %s\n", taskData, userName, userEmail)
	}
}
```



#### Even More Advanced Example Using Programmatic Query Clauses 
```go
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/SamuelBanksTech/Go-Postgresql-Query-Builder/pqb"
	"github.com/jackc/pgx/v4"
)

type SearchFilters struct {
	IncludeAuthorDetails bool
	TitleSearch          string
	AuthorSearch         string
}

func main() {
	urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	defer conn.Close(context.Background())

	filters := SearchFilters{
		IncludeAuthorDetails: true,
		TitleSearch:          "revenge gopher",
		AuthorSearch:         "",
	}

	pgQuery, queryArgs := filterQuery(filters)

	rows, _ := conn.Query(context.Background(), pgQuery, queryArgs...)

	for rows.Next() {
		var bookId int
		var bookTitle string

		if filters.IncludeAuthorDetails {
			var authorName string
			var authorEmail string

			err := rows.Scan(&bookId, &bookTitle, &authorName, &authorEmail)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d - %s - %s - %s\n", bookId, bookTitle, authorName, authorEmail)

		} else {
			err := rows.Scan(&bookId, &bookTitle)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d - %s\n", bookId, bookTitle)

		}
	}
}

func filterQuery(filters SearchFilters) (string, []interface{}) {

	var query pqb.Sqlbuilder

	query.
		From(`myschema.books`).
		Where(`myschema.books.deleted`, `=`, `0`).
		Select(`myschema.books.id`, `myschema.books.title`)

	if filters.IncludeAuthorDetails {
		query.
			LeftJoin(`myschema.authors`, `authors`, `myschema.books.author_id = authors.id`).
			Select(`authors.name`, `authors.email`)
	}

	if len(filters.TitleSearch) > 0 {
		titleSearchWords := strings.Fields(filters.TitleSearch)

		query.WhereStringMatchAny(`myschema.books.title`, titleSearchWords)
	}

	if len(filters.AuthorSearch) > 0 {
		authorSeachWords := strings.Fields(filters.AuthorSearch)

		if !filters.IncludeAuthorDetails {
			query.LeftJoin(`myschema.authors`, `authors`, `myschema.books.author_id = authors.id`)
		}

		query.WhereStringMatchAny(`authors.name`, authorSeachWords)
	}

	return query.Build()
}


```



#### Insert Example
```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/SamuelBanksTech/Go-Postgresql-Query-Builder/pqb"
	"github.com/jackc/pgx/v4"
)

type BookData struct {
	Title string
	Author string `pqb:"writer"` // notice the pqb field tag override
}

func main() {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	bd := BookData{
		Title:  "Revenge of the Gophers",
		Author: "Mr Cool Dev",
	}
	
	var qb pqb.Sqlbuilder
	pgQuery, err := qb.BuildInsert(`myschema.books`, bd, ``)
	if err != nil {
		log.Fatal(err)
    }

	_, err = conn.Exec(context.Background(), pgQuery)
	if err != nil {
		log.Fatal(err)
    }
}
```
Query Output:

`INSERT INTO "myschema"."books" ("title", "writer") VALUES ('Revenge of the Gophers', 'Mr Cool Dev') `
