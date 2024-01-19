/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

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

	t := time.Now()

    // For a time t, offset in seconds east of UTC (GMT)
    _, offset := t.Local().Zone()
    fmt.Println(offset)

    // For a time t, format and display as UTC (GMT) and local times.
    fmt.Println(t.In(time.UTC))
    fmt.Println(t.In(time.Local))

	var secondsEastOfUTC int
	// The localtimezone function returns a []uint8 (".00"), we want it in seconds (an integer)
	selectErr := db.QueryRow("select \"second\"(local_timezone())").Scan(&secondsEastOfUTC)
	switch {
	case selectErr == sql.ErrNoRows:
		println("Query did not return a result")
	case selectErr != nil:
		println(selectErr.Error())
	default:
		println("Value of timezone is", secondsEastOfUTC)
	}
}