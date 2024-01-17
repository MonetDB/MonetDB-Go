/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

package main

import (
	"database/sql"
	"os"

	_ "github.com/MonetDB/MonetDB-Go/src"
)

func main() {
	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	if db == nil {
		println("db is not created")
		os.Exit(1)
	}
	if pingErr := db.Ping(); pingErr != nil {
		println(pingErr.Error())
		os.Exit(1)
	}
	defer db.Close()

	_, err = db.Exec("create table test1 ( name varchar(16), value integer)")
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	result, err := db.Exec("insert into test1 values ( 'name1', 16 )")
	if err != nil {
		println(err.Error())
	}
	if result == nil {
		println("query did not return a result object")
		os.Exit(1)
	}
	rId, err := result.LastInsertId()
	if err != nil {
		println("Could not get id from result")
		os.Exit(1)
	}
	println("Last inserted id", rId)

	nRows, err := result.RowsAffected()
	if err != nil {
		println("Could not get number of rows from result")
		os.Exit(1)
	}
	println("Number of rows", nRows)

	rows, err := db.Query("select * from test1")
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	if rows == nil {
		println("empty result")
		os.Exit(1)
	}
	for rows.Next() {
		var name string
		var val int
		if err := rows.Scan(&name, &val); err != nil {
			println(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		println(err.Error())
	}

	rows, err = db.Query("select * from test1 where name = :name and value = :value", sql.Named("name", "name1"), sql.Named("value", 16))
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	if rows == nil {
		println("empty result")
		os.Exit(1)
	}
	columnlist, err  := rows.Columns()
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	for i, column := range columnlist {
		if i == 0 {
			if column != "name" {
				println("unexpected column name")
			}
		}
		if i == 1 {
			if column != "value" {
				println("unexpected column name")
			}
		}
	}
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			println(err.Error())
			os.Exit(1)
		}
		if name != "name1" {
			println("unexpected value for name field")
		}
		if value != 16 {
			println("unexpected value for value field")
		}
	}
	if err := rows.Err(); err != nil {
		println(err.Error())
	}

	_, err = db.Exec("drop table test1")
	if err != nil {
		println(err.Error())
	}

}
