package main

import (
	"fmt"
	"samuelbanks.net/postgres-query-builder/pqb"
)

func main() {
	var b pqb.Sqlbuilder

	query, args := b.
		From(`myschema.mytable`).
		Where(`name`, `=`, `kirk`).
		Where(`blah`, `=`, `dude`).
		Where(`age`, `>`, `20`).
		Build()

	fmt.Printf("+%v\n", query)
	fmt.Printf("+%v\n", args)
}
