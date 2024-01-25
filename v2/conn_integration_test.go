/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"testing"
	"time"
)

func TestConnIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("Opening the wrong driver should fail", func(t *testing.T) {
		_, err := sql.Open("mysql", "monetdb:monetdb@localhost:50000/monetdb")
		if err != nil {
			t.Log("Impossible to open a connection with a non-existing driver")
		} else {
			t.Fatal(err)
		}
	})

	t.Run("Open should succeed when dsn is correct", func(t *testing.T) {
		db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
		if err != nil {
			t.Fatal(err)
		}
		pingErr := db.Ping()
		if pingErr != nil {
			t.Error(pingErr)
		}
		defer db.Close()
	})

	t.Run("Ping should err when hostname is not correct", func(t *testing.T) {
		db, err := sql.Open("monetdb", "monetdb:monetdb@localhost1:50000/monetdb")
		if err != nil {
			t.Fatal(err)
		}
		if db != nil {
			pingErr := db.Ping()
			if pingErr != nil {
				t.Log("Ping failed as expected")
			} else {
				t.Error("Ping did not fail as expected")
			}
			defer db.Close()
		} else {
			t.Error("Unknown problem with database connection")
		}
	})

	t.Run("Show available drivers", func(t *testing.T) {
		var driverfound = false
		for _, v := range sql.Drivers() {
			if v == "monetdb" {
				driverfound = true
			}
		}
		if !driverfound {
			t.Error("monetdb not in the ist of drivers")
		}
	})

	t.Run("Show number of open connections", func(t *testing.T) {
		db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
		if err != nil {
			t.Fatal(err)
		}
		if db != nil {
			db.Ping()
			if db.Stats().OpenConnections != 1 {
				t.Errorf("Unexpected number of open connections %d", db.Stats().OpenConnections)
			}
			defer db.Close()
		} else {
			t.Error("Unknown problem with database connection")
		}
	})
}

func TestConnSerialIntegration(t *testing.T) {
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
		_, err := db.Exec("create table test3 ( id int AUTO_INCREMENT, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("insert statement", func(t *testing.T) {
		result, err := db.Exec("insert into test3 ( name ) values ( 'name' )" )
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
		if rId != 1 {
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
		rows, err := db.Query("select * from test3 where id = 1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("query returned no rows")
		}
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test3")
		if err != nil {
			t.Error(err)
		}
	})

	defer db.Close()
}

func TestConnTimezoneIntegration(t *testing.T) {
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
	defer db.Close()

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test3 ( sometime timestamptz)")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("insert statement", func(t *testing.T) {
		result, err := db.Exec("insert into test3 ( sometime ) values ( '2024-01-19 09:54:30.988417434+0000' )" )
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
		rows, err := db.Query("select * from test3")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("query returned no rows")
		}
		for rows.Next() {
			var sometime time.Time
			if err := rows.Scan(&sometime); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Run simple query", func(t *testing.T) {
		var secondsEastOfUTC int
		selectErr := db.QueryRow("select \"second\"(local_timezone())").Scan(&secondsEastOfUTC)
		switch {
		case selectErr == sql.ErrNoRows:
			t.Error(selectErr.Error())
		case selectErr != nil:
			t.Error(selectErr.Error())
		default:
			mytime := time.Now()
			_, offset := mytime.Local().Zone()
			if offset != secondsEastOfUTC {
				t.Error("Offset is not the same as the database timezone")
			}
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test3")
		if err != nil {
			t.Error(err)
		}
	})
}