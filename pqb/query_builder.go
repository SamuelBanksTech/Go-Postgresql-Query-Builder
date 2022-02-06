// Copyright 2022 SamuelBanksTech. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pqb

import (
	"errors"
	"fmt"
	"regexp"
	"samuelbanks.net/postgres-query-builder/pqbHelpers"
	"strconv"
	"strings"
)

// Sqlbuilder instanciate this struct and add query parts using attached methods, finally call Build, or use BuildInsert, BuildUpdate, or DeleteFrom
type Sqlbuilder struct {
	string         string
	selectStmt     string
	whereStmt      string
	whereinStmt    string
	fromStmt       string
	deletefromStmt string
	leftjoinStmt   string
	limitStmt      string
	offsetStmt     string
	orderbyStmt    string
	Dialect        string //Can be postgres or mysql atm (more to come)
	Distinct       bool
}

// From portion of query:
// Usage "xxx.From(`myschema.mytable`)"
func (s *Sqlbuilder) From(schemaTable string) *Sqlbuilder {
	s.fromStmt = s.formatSchema(schemaTable)

	return s
}

// DeleteFrom If deleteing from a table use this instead of the above From command
// Usage "xxx.DeleteFrom(`myschema.mytable`)"
func (s *Sqlbuilder) DeleteFrom(schemaTable string) *Sqlbuilder {
	s.deletefromStmt = s.formatSchema(schemaTable)

	return s
}

// SelectRaw query, for use when doing advanced selects (usually CASE WHEN etc) without any helper intervention
// Usage "xxx.From(`myschema.mytable`).SelectRaw(`CASE blah blah blah`)"
func (s *Sqlbuilder) SelectRaw(selectStmt string) *Sqlbuilder {
	re := regexp.MustCompile(`\r?\n`)
	selectStmt = re.ReplaceAllString(selectStmt, " ")

	s.selectStmt += selectStmt + `, `
	return s
}

// Select statement, select the table columns you want returned, no more explanation required surely
// Usage "xxx.From(`myschema.mytable`).Select(`id`, `name`, `telephone`)"
func (s *Sqlbuilder) Select(selectStmt ...string) *Sqlbuilder {

	for _, ss := range selectStmt {
		s.selectStmt += s.formatSchema(ss) + `, `
	}

	return s
}

// Where statement, accepts 3 arguments a column, and operator (can be "=", "!=", ">(=)", "<(=)", "BETWEEN" or any other valid postgres comparison operator)
// You can add as many .Where clauses as you wish they will be treated as AND WHERE
// Usage "xxx.From(`myschema.mytable`).Where(`name`, `=`, `superman`)"
// Usage 2 "xxx.From(`myschema.mytable`).Where(`age`, `BETWEEN`, `20 AND 30`)"
func (s *Sqlbuilder) Where(column string, operator string, value string) *Sqlbuilder {

	operator = strings.ToUpper(operator)
	value = strings.TrimSuffix(value, `'`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `'`)
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimPrefix(value, "`")

	switch operator {
	case `BETWEEN`:
		re := regexp.MustCompile("and|AND|And")
		vp := re.Split(value, -1)
		value = ``

		for _, v := range vp {
			value += pqbHelpers.SanitiseString(`'`+strings.TrimSpace(v)+`'`) + ` AND `
		}

		value = strings.TrimSuffix(value, ` AND `)
	default:
		value = pqbHelpers.SanitiseString(`'` + value + `'`)
	}

	s.whereStmt += s.formatSchema(column) + " " + operator + " " + value + ` AND `

	return s
}

// OrWhere dependant on where it is called it will supersede all other where clauses that have been added before it
// Usage "xxx.From(`myschema.mytable`).Where(`name`, `=`, `superman`).OrWhere(`name`, `=`, `spiderman`)"
func (s *Sqlbuilder) OrWhere(column string, operator string, value string) *Sqlbuilder {

	operator = strings.ToUpper(operator)
	value = strings.TrimSuffix(value, `'`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `'`)
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimPrefix(value, "`")

	switch operator {
	case `BETWEEN`:
		re := regexp.MustCompile("and|AND|And")
		vp := re.Split(value, -1)
		value = ``

		for _, v := range vp {
			value += pqbHelpers.SanitiseString(`'`+strings.TrimSpace(v)+`'`) + ` AND `
		}

		value = strings.TrimSuffix(value, ` AND `)
	default:
		value = pqbHelpers.SanitiseString(`'` + value + `'`)
	}

	s.whereStmt = strings.TrimSuffix(s.whereStmt, ` AND `)
	s.whereStmt += ` OR ` + s.formatSchema(column) + " " + operator + " " + value + ` AND `

	return s
}

// WhereRaw for unfiltered advanced where quires not covered in the above command
// Usage "xxx.From(`myschema.mytable`).WhereRaw(`WHERE SOME COMPLEX QUERY`)"
func (s *Sqlbuilder) WhereRaw(whereStmt string) *Sqlbuilder {
	s.whereStmt += whereStmt + ` AND `
	return s
}

// WhereIn Accepts Slice of INT, FLOAT32, STRING, or a simple comma separated STRING
// Usage "xxx.From(`myschema.mytable`).WhereIn(`age`, []int{20, 25, 30 ,35})"
func (s *Sqlbuilder) WhereIn(column string, params interface{}) *Sqlbuilder {

	output := ""

	switch foo := params.(type) {
	case []int, []float32:
		output += "(" + strings.Trim(strings.Join(strings.Fields(fmt.Sprint(foo)), ", "), "[]") + ")"
		break
	case []string:
		output += "("
		for _, v := range foo {
			output += "'" + pqbHelpers.SanitiseString(v) + "', "
		}
		output = strings.TrimSuffix(output, ", ")
		output += ")"
		break
	case string:
		output = "(" + pqbHelpers.SanitiseString(foo) + ")"
		break
	default:
		output = ""
	}

	if output != "" {
		s.WhereRaw(column + ` IN ` + output)
	}

	return s
}

// WhereStringMatchAny is used for psudo full text search, this function can (case insensitivly) find a string within a string in postgres
// It will return any rows that have at least one of the string in the slice
// Usage "xxx.From(`myschema.mytable`).WhereStringMatchAny(`name`, []string{"bob", "BILLY"})
func (s *Sqlbuilder) WhereStringMatchAny(column string, params []string) *Sqlbuilder {

	output := ""

	output += "(array["
	for _, v := range params {
		output += "'%" + pqbHelpers.SanitiseString(strings.TrimSpace(v)) + "%', "
	}
	output = strings.TrimSuffix(output, ", ")
	output += "])"

	if output != "" {
		s.WhereRaw(column + ` ILIKE ANY ` + output)
	}

	return s
}

// WhereStringMatchAll is used for psudo full text search, this function can (case insensitivly) find a string within a string in postgres
// It will only return rows that have ALL of the strings in the slice
// Usage "xxx.From(`myschema.mytable`).WhereStringMatchAny(`name`, []string{"bob", "BILLY"})
func (s *Sqlbuilder) WhereStringMatchAll(column string, params []string) *Sqlbuilder {

	output := ""

	output += "'"
	for _, v := range params {
		output += "%" + pqbHelpers.SanitiseString(strings.TrimSpace(v)) + "% "
	}
	output = strings.TrimSuffix(output, " ")
	output += "'"

	if output != "" {
		s.WhereRaw(column + ` ILIKE ` + output)
	}

	return s
}

// LeftJoin for joining another table linked by a condition
// Usage "xxx.From(`myschema.mytable`).LeftJoin(`myschema.myothertable`, `mot`, `myschema.mytable.mot_id = mot.id`)
func (s *Sqlbuilder) LeftJoin(table string, as string, on string) *Sqlbuilder {

	table = s.formatSchema(table)
	on = s.formatJoinOn(on)
	as = s.formatSchema(as)

	s.leftjoinStmt += `LEFT JOIN ` + table + ` AS ` + as + ` ON ` + on + ` `
	return s
}

// LeftJoinExtended for joining another table linked by a condition with advance additional commands
// Usage "xxx.From(`myschema.mytable`).LeftJoinExtended(`myschema.myothertable`, `mot`, `myschema.mytable.mot_id = mot.id`, `AND mod.limitingvalue BETWEEN 10 AND 50`)
func (s *Sqlbuilder) LeftJoinExtended(table string, as string, on string, additionalQuery string) *Sqlbuilder {

	table = s.formatSchema(table)
	on = s.formatJoinOn(on)

	s.leftjoinStmt += `LEFT JOIN ` + table + ` AS "` + as + `" ON ` + on + ` ` + additionalQuery + ` `
	return s
}

// Limit the amount of rows returned
// Usage "xxx.From(`myschema.mytable`).Select(`id`, `name`).Limit(10)
func (s *Sqlbuilder) Limit(limit int) *Sqlbuilder {
	s.limitStmt = `LIMIT ` + strconv.Itoa(limit) + ` `

	return s
}

// Offset the selection of rows used in conjustion with limit
// Usage "xxx.From(`myschema.mytable`).Select(`id`, `name`).Limit(10).Offset(20)
func (s *Sqlbuilder) Offset(offset int) *Sqlbuilder {
	s.offsetStmt = `OFFSET ` + strconv.Itoa(offset) + ` `

	return s
}

// OrderBy order the returned rows by a column in ASC (ascending) or DESC (descending) order
// Usage "xxx.From(`myschema.mytable`).Select(`id`, `name`).OrderBy(`id`, `DESC`)
func (s *Sqlbuilder) OrderBy(column string, diretion string) *Sqlbuilder {
	s.orderbyStmt = `ORDER BY "` + column + `" ` + diretion

	return s
}

// Reset clears any previously defined query parts, allows the reuse of an instance
func (s *Sqlbuilder) Reset() *Sqlbuilder {
	s.string = ``
	s.selectStmt = ``
	s.orderbyStmt = ``
	s.whereinStmt = ``
	s.limitStmt = ``
	s.fromStmt = ``
	s.leftjoinStmt = ``
	s.whereStmt = ``
	s.offsetStmt = ``

	return s
}

// Count allows the result of a query to be returned as a numeric amount rather than the actual rows
// You can call count instead of build or you can call count then conditionally call build afterwards
func (s *Sqlbuilder) Count() string {
	sqlquery := s.Build()

	countQuery := `SELECT COUNT(*) AS rowcount FROM (` + sqlquery + `) AS rowdata`

	return countQuery
}
func (s *Sqlbuilder) Exists() string {
	sqlquery := s.Build()

	existsQuery := `SELECT EXISTS (` + sqlquery + `)`

	return existsQuery
}

// Build is the main function of the query builder, it is the final function that takes all the query parts and puts them together
// in a sanitised query ready for passing to a database connection
func (s *Sqlbuilder) Build() string {

	//build selects
	if s.deletefromStmt == `` {

		dis := ""
		if s.Distinct {
			dis = " DISTINCT"
		}

		if s.selectStmt == `` {
			s.string = `SELECT` + dis + ` * `
		} else {
			s.string = `SELECT` + dis + ` ` + strings.TrimSuffix(s.selectStmt, `, `) + ` `
		}
	}

	//build from
	if s.fromStmt == `` {
		if s.deletefromStmt != `` {
			s.string += `DELETE FROM ` + strings.TrimSuffix(s.deletefromStmt, `.`) + ` `
		} else {
			return ``
		}
	} else {
		s.string += `FROM ` + strings.TrimSuffix(s.fromStmt, `.`) + ` `
	}

	//left joins
	s.string += s.leftjoinStmt + ` `

	//where
	if s.whereStmt != `` {
		s.string += `WHERE ` + strings.TrimSuffix(s.whereStmt, ` AND `) + ` `
	}

	//orderby
	if s.orderbyStmt != `` {
		s.string += s.orderbyStmt + ` `
	}

	//limit and offset
	s.string += s.limitStmt
	s.string += s.offsetStmt

	space := regexp.MustCompile(`\s+`)
	s.string = space.ReplaceAllString(s.string, " ")

	returnString := s.string

	return returnString
}

// BuildInsert is a very simple yet powerful feature that saves a lot of time, you simply pass a schema and table ref and a struct of data
// the builder will automatically build the insert based on the struct value names and values if the field tag of "pqb" is used one can
// override the struct name
func (s *Sqlbuilder) BuildInsert(table string, data interface{}, additionalQuery string) (string, error) {
	dbCols, dbVals, err := pqbHelpers.MapStruct(data)
	if err != nil {
		return "", err
	}

	sql := "INSERT INTO " + s.formatSchema(table) + " (" + strings.Join(dbCols, ", ") + ") VALUES (" + strings.Join(dbVals, ", ") + ") " + additionalQuery

	return sql, nil
}

// BuildUpdate like the buildinsert takes a table and a struct of data, however unlike buildinsert buildupdate will look to replace all
// matching column names always best to ensure to use a Where query part to avoid accidental data loss
func (s *Sqlbuilder) BuildUpdate(table string, data interface{}) (string, error) {

	dbCols, dbVals, err := pqbHelpers.MapStruct(data)
	if err != nil {
		return "", err
	}

	setString := ""
	sql := ""

	for i, col := range dbCols {
		setString += col + ` = ` + dbVals[i] + `, `
	}
	setString = strings.TrimSuffix(setString, `, `) + ` `

	if setString != "" {
		sql = "UPDATE " + s.formatSchema(table) + ` SET ` + setString

		if s.whereStmt != `` {
			sql += `WHERE ` + strings.TrimSuffix(s.whereStmt, ` AND `) + ` `
		}

		return sql, nil
	}

	return sql, errors.New("sql build failed")
}

// Based upon dialect this function will split a string schema-table reference into the correct
// format required. e.g. `myschema.mytable` into "myschema"."mytable"
func (s *Sqlbuilder) formatSchema(schema string) string {
	schemaParts := strings.Split(schema, ".")
	finalSchemaStmt := ``

	var dialectFormat string

	switch strings.ToLower(s.Dialect) {
	case "postgres":
		dialectFormat = `"`
		break
	case "mysql":
		dialectFormat = "`"
		break
	default:
		dialectFormat = `"`
	}

	for _, v := range schemaParts {
		if v == `*` {
			finalSchemaStmt += `*`
		} else {
			part := strings.TrimSpace(v)
			if string(part[0]) == dialectFormat && string(part[len(part)-1]) == dialectFormat {
				finalSchemaStmt += part + `.`
			} else {
				finalSchemaStmt += dialectFormat + part + dialectFormat + `.`
			}

		}
	}

	return strings.TrimSuffix(finalSchemaStmt, `.`)
}

// used to ensure the correct formatting for the ON part of a join query
func (s *Sqlbuilder) formatJoinOn(joinStmt string) string {
	joinParts := strings.Split(joinStmt, "=")
	finalJoinStmt := ``

	for _, v := range joinParts {
		finalJoinStmt += s.formatSchema(v) + ` = `
	}

	return strings.TrimSuffix(finalJoinStmt, ` = `)
}
