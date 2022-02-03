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
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

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

// If deleteing from a table use this instead of the above From command
// Usage "xxx.DeleteFrom(`myschema.mytable`)"
func (s *Sqlbuilder) DeleteFrom(schemaTable string) *Sqlbuilder {
	s.deletefromStmt = s.formatSchema(schemaTable)

	return s
}

// Raw Select query, for use when doing advanced selects (usually CASE WHEN etc) without any helper intervention
// Usage "xxx.From(`myschema.mytable`).SelectRaw(`CASE blah blah blah blah`)"
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
			value += sanitiseString(`'`+strings.TrimSpace(v)+`'`) + ` AND `
		}

		value = strings.TrimSuffix(value, ` AND `)
	default:
		value = sanitiseString(`'` + value + `'`)
	}

	s.whereStmt += s.formatSchema(column) + " " + operator + " " + value + ` AND `

	return s
}

// WhereRaw for unfiltered advanced where quires not covered in the above command
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
			output += "'" + sanitiseString(v) + "', "
		}
		output = strings.TrimSuffix(output, ", ")
		output += ")"
		break
	case string:
		output = "(" + sanitiseString(foo) + ")"
		break
	default:
		output = ""
	}

	if output != "" {
		s.WhereRaw(column + ` IN ` + output)
	}

	return s
}

// Used for psudo full text search, this funtion can (case insensitivly) find a string within a string in postgres
// It will return any rows that have at least one of the string in the slice
// Usage "xxx.From(`myschema.mytable`).WhereStringMatchAny(`name`, []string{"bob", "BILLY"})
func (s *Sqlbuilder) WhereStringMatchAny(column string, params []string) *Sqlbuilder {

	output := ""

	output += "(array["
	for _, v := range params {
		output += "'%" + sanitiseString(strings.TrimSpace(v)) + "%', "
	}
	output = strings.TrimSuffix(output, ", ")
	output += "])"

	if output != "" {
		s.WhereRaw(column + ` ILIKE ANY ` + output)
	}

	return s
}

// Used for psudo full text search, this funtion can (case insensitivly) find a string within a string in postgres
// It will only return rows that have ALL of the strings in the slice
// Usage "xxx.From(`myschema.mytable`).WhereStringMatchAny(`name`, []string{"bob", "BILLY"})
func (s *Sqlbuilder) WhereStringMatchAll(column string, params []string) *Sqlbuilder {

	output := ""

	output += "'"
	for _, v := range params {
		output += "%" + sanitiseString(strings.TrimSpace(v)) + "% "
	}
	output = strings.TrimSuffix(output, " ")
	output += "'"

	if output != "" {
		s.WhereRaw(column + ` ILIKE ` + output)
	}

	return s
}

func (s *Sqlbuilder) LeftJoin(table string, as string, on string) *Sqlbuilder {

	table = s.formatSchema(table)
	on = s.formatJoinOn(on)
	as = s.formatSchema(as)

	s.leftjoinStmt += `LEFT JOIN ` + table + ` AS ` + as + ` ON ` + on + ` `
	return s
}

func (s *Sqlbuilder) LeftJoinExtended(table string, as string, on string, additionalQuery string) *Sqlbuilder {

	table = s.formatSchema(table)
	on = s.formatJoinOn(on)

	s.leftjoinStmt += `LEFT JOIN ` + table + ` AS "` + as + `" ON ` + on + ` ` + additionalQuery + ` `
	return s
}

func (s *Sqlbuilder) Limit(limit int) *Sqlbuilder {
	s.limitStmt = `LIMIT ` + strconv.Itoa(limit) + ` `

	return s
}

func (s *Sqlbuilder) Offset(offset int) *Sqlbuilder {
	s.offsetStmt = `OFFSET ` + strconv.Itoa(offset) + ` `

	return s
}

func (s *Sqlbuilder) OrderBy(column string, diretion string) *Sqlbuilder {

	s.orderbyStmt = `ORDER BY "` + column + `" ` + diretion

	return s
}

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

func (s *Sqlbuilder) BuildInsert(table string, data interface{}, additionalQuery string) (string, error) {
	dbCols, dbVals, err := mapStruct(data)
	if err != nil {
		return "", err
	}

	sql := "INSERT INTO " + s.formatSchema(table) + " (" + strings.Join(dbCols, ", ") + ") VALUES (" + strings.Join(dbVals, ", ") + ") " + additionalQuery

	return sql, nil
}

func (s *Sqlbuilder) BuildUpdate(table string, data interface{}, additionalQuery string) (string, error) {

	dbCols, dbVals, err := mapStruct(data)
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

		sql += additionalQuery

		return sql, nil
	}

	return sql, errors.New("sql build failed")
}

// Based upon dialect this funtion will split a string schema-table reference into the correct
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

func (s *Sqlbuilder) formatJoinOn(joinStmt string) string {
	joinParts := strings.Split(joinStmt, "=")
	finalJoinStmt := ``

	for _, v := range joinParts {
		finalJoinStmt += s.formatSchema(v) + ` = `
	}

	return strings.TrimSuffix(finalJoinStmt, ` = `)
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func mapStruct(data interface{}) (dbCols []string, dbVals []string, error error) {
	fields := reflect.TypeOf(data)
	values := reflect.ValueOf(data)

	num := fields.NumField()

	for i := 0; i < num; i++ {
		field := fields.Field(i)
		value := values.Field(i)

		val, exists := field.Tag.Lookup("sqlb")

		if exists {
			dbCols = append(dbCols, "\""+val+"\"")
		} else {
			dbCols = append(dbCols, "\""+toSnakeCase(field.Name)+"\"")
		}

		var v string

		switch value.Kind() {
		case reflect.String:
			v = "'" + sanitiseString(value.String()) + "'"
		case reflect.Int:
			v = strconv.FormatInt(value.Int(), 10)
		case reflect.Int8:
			v = strconv.FormatInt(value.Int(), 10)
		case reflect.Int32:
			v = strconv.FormatInt(value.Int(), 10)
		case reflect.Int64:
			v = strconv.FormatInt(value.Int(), 10)
		case reflect.Float64:
			v = fmt.Sprintf("%f", value.Float())
		case reflect.Float32:
			v = fmt.Sprintf("%f", value.Float())
		case reflect.Bool:
			if value.Bool() {
				v = "TRUE"
			} else {
				v = "FALSE"
			}
		default:
			return dbCols, dbVals, errors.New("type: " + value.Kind().String() + " unsupported")
		}

		dbVals = append(dbVals, v)
	}

	return dbCols, dbVals, nil
}

func sanitiseString(str string) string {

	if len(str) > 0 {
		rebuildSingles := false

		if string(str[0]) == "'" && string(str[len(str)-1]) == "'" {
			rebuildSingles = true
		}

		str = strings.TrimSuffix(strings.TrimPrefix(str, "'"), "'")
		str = strings.ReplaceAll(str, "'", "''")

		if rebuildSingles {
			str = "'" + str + "'"
		}
	}

	return str
}
