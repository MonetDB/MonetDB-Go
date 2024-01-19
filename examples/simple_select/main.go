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

	_, createErr := db.Exec("create table test2 ( id int, name varchar(16))")
	if createErr != nil {
		println(createErr.Error())
	}

	result, err := db.Exec("insert into test2 values ( 1, 'name1' )")
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
	println("Result returned id", rId)

	nRows, err := result.RowsAffected()
	if err != nil {
		println("Could not get number of rows from result")
		os.Exit(1)
	}
	println("Result returned number of rows", nRows)

	var name string
	selectErr := db.QueryRow("select name from test2 where id = 1").Scan(&name)
	switch {
	case selectErr == sql.ErrNoRows:
		println("Query did not return a result")
	case selectErr != nil:
		println(selectErr.Error())
	default:
		println("Value of name is", name)
	}

	_, dropErr := db.Exec("drop table test2")
	if dropErr != nil {
		println(dropErr.Error())
	}

}
