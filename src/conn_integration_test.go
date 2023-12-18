/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"testing"
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