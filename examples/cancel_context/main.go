/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package main

import (
	"context"
	"database/sql"
	"os"
	"time"

	_ "github.com/MonetDB/MonetDB-Go/src"
)
  
var (
	ctx context.Context
	cancel context.CancelFunc
)

func main () {
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

	_, createErr := db.Exec("create procedure sleep(i int) external name alarm.sleep")
	if createErr != nil {
		println(createErr.Error())
	}

	println("Run a query that is cancelled by a timeout")
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	if _, execErr := db.ExecContext(ctx, "CALL sys.sleep(200)"); execErr != nil {
		println(execErr.Error())
	}
	if ctx.Err() != nil {
		println(ctx.Err().Error())
	}

	println("Run a query that finishes before the timeout")
	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	
	if _, execErr := db.ExecContext(ctx, "CALL sys.sleep(200)"); execErr != nil {
		println(execErr.Error())
	} else {
		println("The query finished before the timeout")
		if ctx.Err() != nil {
			println(ctx.Err().Error())
		}
	}

	println("Run a query that gives an error")
	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	
	if _, execErr := db.ExecContext(ctx, "select * from test1"); execErr != nil {
		println(execErr.Error())
	} else {
		println("The query finished before the timeout")
		if ctx.Err() != nil {
			println(ctx.Err().Error())
		}
	}

	println("Run a query that returns a result")
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var name int
	selectErr := db.QueryRowContext(ctx, "select 42").Scan(&name)
	switch {
		case selectErr == sql.ErrNoRows:
			println("Query did not return a result")
		case selectErr != nil:
			println(selectErr.Error())
		default:
			println("Value of name is", name)
	}

	_, dropErr := db.Exec("drop procedure sleep")
	if dropErr != nil {
		println(dropErr.Error())
	}
}