/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"fmt"
	"math"
	"testing"
)
 
func TestRowsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		result, err := db.Exec("create table test1 ( name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 'name1' )")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != -1 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Error("unexpected column name")
			}
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		result, err := db.Exec("drop table test1")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	defer db.Close()
}


func TestColumnTypesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	type coltypetest struct {
		ct    string // create table query
		it    string // insert into query
		cs    string // select star query
		cn  []string // column names
		lok []bool   // length value available
		cl  []int64  // column lengths
		nok []bool   // nullable available
		ctn []string // column type name
		st  []string // go type of column
		ds  []bool   // decimal size available
		dsp []int64  // decimal precision
		dss []int64  // decimal scale
		dt   string  // drop table query
	}

	var ctl = []coltypetest{
		{
			"create table test1 ( name varchar(16))",
			"insert into test1 values ( 'name1' )",
			"select * from test1",
			[]string{"name"},
			[]bool{true},
			[]int64{16},
			[]bool{false},
			[]string{"VARCHAR"},
			[]string{"string"},
			[]bool{false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( value int)",
			"insert into test1 values ( 25 )",
			"select * from test1",
			[]string{"value"},
			[]bool{false},
			[]int64{0},
			[]bool{false},
			[]string{"INT"},
			[]string{"int32"},
			[]bool{false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name varchar(16), value int)",
			"insert into test1 values ( 'name1', 25 )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{true, false},
			[]int64{16, 0},
			[]bool{false, false},
			[]string{"VARCHAR", "INT"},
			[]string{"string", "int32"},
			[]bool{false, false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name varchar(32), value bigint)",
			"insert into test1 values ( 'name1', 25 )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{true, false},
			[]int64{32, 0},
			[]bool{false, false},
			[]string{"VARCHAR", "BIGINT"},
			[]string{"string", "int64"},
			[]bool{false, false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name blob, value boolean )",
			"insert into test1 values ( x'1a2b3c4d5e', true )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{true, false},
			[]int64{math.MaxInt64, 0},
			[]bool{false, false},
			[]string{"BLOB", "BOOLEAN"},
			[]string{"[]uint8", "bool"},
			[]bool{false, false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name real, value boolean)",
			"insert into test1 values ( 1.2345, true )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{false, false},
			[]int64{0, 0},
			[]bool{false, false},
			[]string{"REAL", "BOOLEAN"},
			[]string{"float32", "bool"},
			[]bool{false, false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name smallint, value double)",
			"insert into test1 values ( 12, 1.2345 )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{false, false},
			[]int64{0, 0},
			[]bool{false, false},
			[]string{"SMALLINT", "DOUBLE"},
			[]string{"int16", "float64"},
			[]bool{false, false},
			[]int64{0, 0},
			[]int64{0, 0},
			"drop table test1",
		},
		{
			"create table test1 ( name decimal, value decimal(10, 5))",
			"insert into test1 values ( 1.2345, 67.890 )",
			"select * from test1",
			[]string{"name", "value"},
			[]bool{false, false},
			[]int64{0, 0},
			[]bool{false, false},
			[]string{"DECIMAL", "DECIMAL"},
			[]string{"float64", "float64"},
			[]bool{true, true},
			[]int64{18, 10},
			[]int64{3, 5},
			"drop table test1",
		},
		{
			"create table test1 ( name timestamptz)",
			"insert into test1 values ( current_timestamp() )",
			"select * from test1",
			[]string{"name"},
			[]bool{false},
			[]int64{0},
			[]bool{false},
			[]string{"TIMESTAMPTZ"},
			[]string{"Time"},
			[]bool{false},
			[]int64{0},
			[]int64{0},
			"drop table test1",
		},
	}

	for i := range ctl {
		t.Run("Exec create table", func(t *testing.T) {
			_, err := db.Exec(ctl[i].ct)
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Exec insert row", func(t *testing.T) {
			_, err := db.Exec(ctl[i].it)
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Get Columns", func(t *testing.T) {
			rows, err := db.Query(ctl[i].cs)
			if err != nil {
				t.Fatal(err)
			}
			if rows == nil {
				t.Fatal("empty result")
			}
			columnlist, err  := rows.Columns()
			if err != nil {
				t.Error(err)
			}
			for j, column := range columnlist {
				if column !=  ctl[i].cn[j]{
					t.Errorf("unexpected column name in Columns: %s", column)
				}
			}
			columntypes, err  := rows.ColumnTypes()
			if err != nil {
				t.Error(err)
			}
			for j, column := range columntypes {
				if column.Name() != ctl[i].cn[j] {
					t.Errorf("unexpected column name in ColumnTypes")
				}
				length, length_ok := column.Length()
				if length_ok != ctl[i].lok[j] {
					t.Errorf("unexpected value for length_ok")
				} else {
					if length_ok {
						if length != ctl[i].cl[j] {
							t.Errorf("unexpected column length in ColumnTypes")
						}
					}
				}
				_, nullable_ok := column.Nullable()
				if nullable_ok != ctl[i].nok[j]{
					t.Errorf("not expected that nullable was provided")
				}
				coltype := column.DatabaseTypeName()
				if coltype != ctl[i].ctn[j] {
					t.Errorf("unexpected column typename")
				}
				scantype := column.ScanType()
				// Not every type has a name. Then the name is the empty string. In that case, compare the types
				if scantype.Name() != "" {
					if scantype.Name() != ctl[i].st[j] {
						t.Errorf("unexpected scan type: %s instead of %s", ctl[i].st[j], scantype.Name())
					}
				} else {
					if fmt.Sprintf("%v", scantype) != ctl[i].st[j] {
						t.Errorf("unexpected scan type: %s instead of %v", ctl[i].st[j], scantype)
					}
				}
				precision, scale, ok := column.DecimalSize()
				if ok != ctl[i].ds[j]{
					t.Errorf("not expected that decimal size was provided")
				} else {
					if ok {
						if precision != ctl[i].dsp[j] {
							t.Errorf("Unexpected value for precision")
						}
						if scale != ctl[i].dss[j] {
							t.Errorf("unexpected value for scale")
						}
					}
				}
			}
			/*
			for rows.Next() {
				name := make([]driver.Value, colcount)
				if err := rows.Scan(&name); err != nil {
				t.Error(err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			}
			*/
			defer rows.Close()
		})

		t.Run("Exec drop table", func(t *testing.T) {
			_, err := db.Exec(ctl[i].dt)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	defer db.Close()
}
