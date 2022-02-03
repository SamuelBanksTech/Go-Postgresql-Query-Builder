# Postgresql Query Builder for Go

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

	pgQuery := qb.
		From(`myschema.widgets`).
		Select(`name`, `weight`).
		Where(`id`, `=`, `1`).
		Build()

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), pgQuery).Scan(&name, &weight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(name, weight)
}
```
Query Output:

`SELECT "name", "weight" FROM "myschema"."widgets" WHERE "id" = '1'`



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

	pgQuery := qb.
		From(`myschema.tasks`).
		LeftJoin(`myschema.users`, `users`, `myschema.tasks.user_id = users.id`).
		Where(`users.active`, `=`, `1`).
		Where(`myschema.tasks.completed`, `=`, `0`).
		Select(`myschema.tasks.task_details`, `users.name`, `users.email`).
		Build()


	rows, _ := conn.Query(context.Background(), pgQuery)

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
Query Output:

`SELECT "myschema"."tasks"."task_details", "users"."name", "users"."email" FROM "myschema"."tasks" LEFT JOIN "myschema"."users" AS "users" ON "myschema"."tasks"."user_id" = "users"."id" WHERE "users"."active" = '1' AND "myschema"."tasks"."completed" = '0'`


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