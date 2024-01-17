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

	_, err = db.Exec("create table test3 ( id int, name varchar(16))")
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	stmt, err := db.Prepare("insert into test3 values ( ?, ? )")
	if err != nil {
		println(err.Error())
	}
	if stmt == nil {
		println("Statement is nil")
		os.Exit(1)
	}
	defer stmt.Close()

	result, err := stmt.Exec(1, "name1" )
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

	stmt, err = db.Prepare("select * from test3 where id = ?")
	if err != nil {
		println(err.Error())
	}
	if stmt == nil {
		println("Statement is nil")
		os.Exit(1)
	}
	defer stmt.Close()

	rows, err := stmt.Query(1)
	if err != nil {
		println(err.Error())
	}
	if rows == nil {
		println("query returned no rows")
		os.Exit(1)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			println(err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		println(err.Error())
	}

	_, err = db.Exec("drop table test3")
	if err != nil {
		println(err.Error())
	}
}