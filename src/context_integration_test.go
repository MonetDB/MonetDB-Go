/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"context"
	"testing"
)
  
func TestContextDBIntegration(t *testing.T) {
	// This test verifies that the *Context methods work without the monetdb driver having
	// to implement anything specific. This means that any value in the context is not used
	// by the driver.
	if testing.Short() {
		t.Skip("skipping integration test")
	}
 
	var (
		ctx context.Context
		db  *sql.DB		
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}
 
	t.Run("Exec create table", func(t *testing.T) {
		result, err := db.ExecContext(ctx, "create table test1 ( name varchar(16))")
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
		result, err := db.ExecContext(ctx, "insert into test1 values ( 'name1' )")
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
		rows, err := db.QueryContext(ctx, "select * from test1")
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
		rows, err := db.QueryContext(ctx, "select * from test1")
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
		result, err := db.ExecContext(ctx, "drop table test1")
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
}
func TestContextConnIntegration(t *testing.T) {
	// This test verifies that you can request a connection object and use that to 
	// run queries. But the preferred way is to use the DB object directly, according
	// to the documentation.
	if testing.Short() {
		t.Skip("skipping integration test")
	}
 
	var (
		ctx context.Context
		db  *sql.DB		
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}
 
	t.Run("Exec create table", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() // Return the connection to the pool.

		if pingErr := conn.PingContext(ctx); pingErr != nil {
			t.Fatal(pingErr)
		}
	
		result, err := conn.ExecContext(ctx, "create table test1 ( name varchar(16))")
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
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := conn.ExecContext(ctx, "insert into test1 values ( 'name1' )")
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
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := conn.QueryContext(ctx, "select * from test1")
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
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := conn.QueryContext(ctx, "select * from test1")
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
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := conn.ExecContext(ctx, "drop table test1")
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
}

func TestContextTxIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var (
		ctx context.Context
		db  *sql.DB		
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "create table test4 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Start transaction", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.ExecContext(ctx, "insert into test4 values ( 1, 'name1' )")
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
		if err := tx.Commit(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "select * from test4")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
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
		_, err := db.ExecContext(ctx, "drop table test4")
		if err != nil {
			t.Fatal(err)
		}
	})

	defer db.Close()
}

func TestContextStmtIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var (
		ctx context.Context
		db  *sql.DB		
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}
 
	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "create table test3 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Prepare statement", func(t *testing.T) {

		stmt, err := db.PrepareContext(ctx, "insert into test3 values ( ?, ? )")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		result, err := stmt.ExecContext(ctx, 1, "name1" )
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
		stmt, err := db.PrepareContext(ctx, "select * from test3 where id = ?")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		rows, err := stmt.QueryContext(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

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
	})

	t.Run("Get Columns", func(t *testing.T) {
		stmt, err := db.PrepareContext(ctx, "select * from test3 where id = ?")
		if err != nil {
			t.Logf(err.Error())
		}
		defer stmt.Close()

		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		rows, err := stmt.QueryContext(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("query returned empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Logf(err.Error())
		}
		for i, column := range columnlist {
			if i == 0 && column != "id" {
				t.Errorf("Name of column %d not correct", i)
			}
			if i == 1 && column != "name" {
				t.Errorf("Name of column %d not correct", i)
			}
		} 
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "drop table test3")
		if err != nil {
			t.Error(err)
		}
	})

	defer db.Close()
}

func TestContextRowIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var (
		ctx context.Context
		db  *sql.DB
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "create table test2 ( id int, name varchar(16))")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.ExecContext(ctx, "insert into test2 values ( 1, 'name1' )");
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
		var name string
		err := db.QueryRowContext(ctx, "select name from test2 where id = 1").Scan(&name)
		switch {
		case err == sql.ErrNoRows:
			t.Error("Query did not return a result")
		case err != nil:
			t.Error(err)
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "drop table test2")
		if err != nil {
			t.Error(err)
		}
	})
}
