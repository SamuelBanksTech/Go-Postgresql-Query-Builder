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

package pqbHelpers

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// ToSnakeCase simple function to return snake case from a string
func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// MapStruct takes a struct of data and outputs two string slices (regardless of struct values)
// first is the db column names taken from the stuct names or overridden if the field tag "pqb" is used on the struct value
// the second is the value in the corresponding index
func MapStruct(data interface{}) (dbCols []string, dbVals []string, error error) {
	fields := reflect.TypeOf(data)
	values := reflect.ValueOf(data)

	num := fields.NumField()

	for i := 0; i < num; i++ {
		field := fields.Field(i)
		value := values.Field(i)

		val, exists := field.Tag.Lookup("pqb")

		if exists {
			dbCols = append(dbCols, "\""+val+"\"")
		} else {
			dbCols = append(dbCols, "\""+ToSnakeCase(field.Name)+"\"")
		}

		var v string

		switch value.Type().String() {
		case "string":
			v = "'" + SanitiseString(value.String()) + "'"
		case "int":
			v = strconv.FormatInt(value.Int(), 10)
		case "int8":
			v = strconv.FormatInt(value.Int(), 10)
		case "int32":
			v = strconv.FormatInt(value.Int(), 10)
		case "int64":
			v = strconv.FormatInt(value.Int(), 10)
		case "float64":
			v = fmt.Sprintf("%f", value.Float())
		case "float32":
			v = fmt.Sprintf("%f", value.Float())
		case "time.Time":
			tr := value.Interface()
			t := tr.(time.Time)
			v = t.Format("2006-01-02 15:04:05")
		case "bool":
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

// SanitiseString this is the first step in adding some much needed security
// right now this just cleans end ensures user strings a clean
func SanitiseString(str string) string {

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
