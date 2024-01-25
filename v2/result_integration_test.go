/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"testing"
)

 func TestResultIntegration(t *testing.T) {
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

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("SELECT 42")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("query did not return a rows object")
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

	t.Run("Exec simple query", func(t *testing.T) {
		result, err := db.Exec("SELECT 42")
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
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	defer db.Close()
}