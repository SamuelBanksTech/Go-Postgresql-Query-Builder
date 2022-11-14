package pqb

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSqlbuilder_From_and_Build(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, _ := sqlb.From(`myschema.mytable`).Build()

	wantSql := `SELECT * FROM "myschema"."mytable"`

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}
}

func TestSqlbuilder_Select(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, _ := sqlb.From(`myschema.mytable`).Select(`mycol1`, `mycol2`).Build()

	wantSql := `SELECT "mycol1", "mycol2" FROM "myschema"."mytable"`

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}
}

func TestSqlbuilder_SelectRaw(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, _ := sqlb.From(`myschema.mytable`).
		Select(`mycol`).
		SelectRaw(`CASE WHEN mycol > 0 AND mycol <= 50 THEN 'small' WHEN mycol > 50 THEN 'large' END size`).
		Build()

	wantSql := `SELECT "mycol", CASE WHEN mycol > 0 AND mycol <= 50 THEN 'small' WHEN mycol > 50 THEN 'large' END size FROM "myschema"."mytable"`

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}
}

func TestSqlbuilder_DeleteFrom(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, _ := sqlb.DeleteFrom(`myschema.mytable`).
		Where(`mycol`, `=`, `1`).
		Build()

	wantSql := `DELETE FROM "myschema"."mytable" WHERE "mycol" = $1`

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}
}

func TestSqlbuilder_Where(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, gotArgs := sqlb.From(`myschema.mytable`).
		Where(`mycol`, `=`, `true`).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" = $1`
	wantArgs := []string{`true`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != 1 {
		t.Error(`argument slice length wrong`)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_OrWhere(t *testing.T) {
	var sqlb Sqlbuilder
	gotSql, gotArgs := sqlb.From(`myschema.mytable`).
		Where(`mycol`, `=`, `true`).
		OrWhere(`mycol2`, `=`, `somevalue`).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" = $1 OR "mycol2" = $2`
	wantArgs := []string{`true`, `somevalue`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != 2 {
		t.Error(`argument slice length wrong`)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_WhereIn(t *testing.T) {
	var sqlb Sqlbuilder

	var wantArgsTests []interface{}

	wantArgsTests = append(wantArgsTests, []string{`abc`, `def`, `ghi`})
	wantArgsTests = append(wantArgsTests, []int{1, 2, 3})
	wantArgsTests = append(wantArgsTests, []float32{1.1, 1.2, 1.3})
	wantArgsTests = append(wantArgsTests, []float64{2.1, 2.2, 2.3})

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" IN ($1, $2, $3)`

	for _, wa := range wantArgsTests {

		switch wantArgs := wa.(type) {
		case []int:
			gotSql, gotArgs := sqlb.Reset().
				From(`myschema.mytable`).
				WhereIn(`mycol`, wantArgs).
				Build()

			if gotSql != wantSql {
				t.Errorf("IntSlice: \ngot %v \nwanted %v", gotSql, wantSql)
			}

			if len(gotArgs) != len(wantArgs) {
				t.Error(`IntSlice: argument slice length wrong`)
			}

			for i, v := range wantArgs {
				if gotArgs[i] != strconv.Itoa(v) {
					t.Errorf("IntSlice: argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
				}
			}
			break
		case []string:
			gotSql, gotArgs := sqlb.Reset().
				From(`myschema.mytable`).
				WhereIn(`mycol`, wantArgs).
				Build()

			if gotSql != wantSql {
				t.Errorf("StringSlice: \ngot %v \nwanted %v", gotSql, wantSql)
			}

			if len(gotArgs) != len(wantArgs) {
				t.Error(`StringSlice: argument slice length wrong`)
			}

			for i, v := range wantArgs {
				if gotArgs[i] != v {
					t.Errorf("StringSlice: argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
				}
			}
			break
		case []float32:
			gotSql, gotArgs := sqlb.Reset().
				From(`myschema.mytable`).
				WhereIn(`mycol`, wantArgs).
				Build()

			if gotSql != wantSql {
				t.Errorf("Float32Slice: \ngot %v \nwanted %v", gotSql, wantSql)
			}

			if len(gotArgs) != len(wantArgs) {
				t.Error(`Float32Slice: argument slice length wrong`)
			}

			for i, v := range wantArgs {
				if gotArgs[i] != fmt.Sprintf("%f", v) {
					t.Errorf("Float32Slice: argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
				}
			}
			break
		case []float64:
			gotSql, gotArgs := sqlb.Reset().
				From(`myschema.mytable`).
				WhereIn(`mycol`, wantArgs).
				Build()

			if gotSql != wantSql {
				t.Errorf("Float64Slice: \ngot %v \nwanted %v", gotSql, wantSql)
			}

			if len(gotArgs) != len(wantArgs) {
				t.Error(`Float64Slice: argument slice length wrong`)
			}

			for i, v := range wantArgs {
				if gotArgs[i] != fmt.Sprintf("%f", v) {
					t.Errorf("Float64Slice: argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
				}
			}
			break

		}
	}

}

func TestSqlbuilder_WhereStringMatchAny(t *testing.T) {
	var sqlb Sqlbuilder

	gotSql, gotArgs := sqlb.From(`myschema.mytable`).
		WhereStringMatchAny(`mycol`, []string{`abc`, `def`}).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" ILIKE ANY (array[$1, $2])`
	wantArgs := []string{`%abc%`, `%def%`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != 2 {
		t.Error(`argument slice length wrong`)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_WhereStringMatchAll(t *testing.T) {
	var sqlb Sqlbuilder

	gotSql, gotArgs := sqlb.From(`myschema.mytable`).
		WhereStringMatchAll(`mycol`, []string{`abc`, `def`}).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" ILIKE ALL (array[$1, $2])`
	wantArgs := []string{`%abc%`, `%def%`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != 2 {
		t.Error(`argument slice length wrong`)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_Limit_Offset_OrderBy(t *testing.T) {
	var sqlb Sqlbuilder

	gotSql, gotArgs := sqlb.From(`myschema.mytable`).
		Where(`mycol`, `=`, `abc`).
		Limit(10).
		Offset(10).
		OrderBy(`mycol2`, `ASC`).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" = $1 ORDER BY "mycol2" ASC LIMIT 10 OFFSET 10`
	wantArgs := []string{`abc`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != len(wantArgs) {
		t.Error(`argument slice length wrong`)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_LeftJoin(t *testing.T) {
	var sqlb Sqlbuilder

	gotSql, _ := sqlb.From(`myschema.mytable`).
		LeftJoin(`mycol`, `mc`, `myschema.mytable.id = mc.mt_id`).
		Build()

	wantSql := `SELECT * FROM "myschema"."mytable" LEFT JOIN "mycol" AS "mc" ON "myschema"."mytable"."id" = "mc"."mt_id"`

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

}

func TestSqlbuilder_Reset(t *testing.T) {
	var sqlb Sqlbuilder
	_, _ = sqlb.From(`myschema.myoldtable`).
		Select(`mycol1`, `mycol2`).
		Where(`myoldcol`, `=`, `someoldvalue`).
		WhereIn(`mycol3`, []string{`abc`, `def`}).
		LeftJoin(`myschema.mypivot`, `mp`, `mp.id = myoldtable.mp_id`).
		Limit(10).
		Offset(20).
		OrderBy(`mycol1`, `ASC`).
		Build()

	gotSql, gotArgs := sqlb.Reset().From(`myschema.mytable`).Where(`mycol`, `=`, `true`).Build()

	wantSql := `SELECT * FROM "myschema"."mytable" WHERE "mycol" = $1`
	wantArgs := []string{`true`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != 1 {
		t.Errorf("argument slice length wrong: \ngot %v \nwanted %v", gotArgs, wantArgs)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got \ngot %v \nwanted %v as position %v", gotArgs[i], wantArgs[i], i)
		}
	}
}

func TestSqlbuilder_BuildInsert_and_mapping(t *testing.T) {
	var sqlb Sqlbuilder

	nowTime := time.Now()

	mockStruct := struct {
		StringCol        string
		StringColNewName string `pqb:"scnn"`
		IntCol           int
		Int8Col          int8
		Int32Col         int32
		Int64Col         int64
		Float64Col       float64
		Float32Col       float32
		TimeCol          time.Time
		BoolCol          bool
	}{
		"mystring",
		"myscnnstring",
		1,
		2,
		3,
		4,
		1.1,
		1.2,
		nowTime,
		true,
	}

	gotSql, gotArgs, err := sqlb.BuildInsert(`myschema.mytable`, mockStruct, `ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Error(err)
	}

	wantSql := `INSERT INTO "myschema"."mytable" ("string_col", "scnn", "int_col", "int8_col", "int32_col", "int64_col", "float64_col", "float32_col", "time_col", "bool_col") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING`
	wantArgs := []string{`'mystring'`, `'myscnnstring'`, `1`, `2`, `3`, `4`, `1.100000`, `1.200000`, nowTime.Format("2006-01-02 15:04:05"), `TRUE`}

	if gotSql != wantSql {
		t.Errorf("got %v \nwanted %v", gotSql, wantSql)
	}

	if len(gotArgs) != len(wantArgs) {
		t.Errorf("length of args incorrect: \ngot %v \nwanted %v", gotArgs, wantArgs)
	}

	for i, v := range wantArgs {
		if gotArgs[i] != v {
			t.Errorf("argument mismatch got ")
		}
	}
}
