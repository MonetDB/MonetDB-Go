/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"testing"
)

func TestResultSet(t *testing.T) {
	t.Run("Verify createExecString with empty arg list", func(t *testing.T) {
		var r ResultSet
		arglist := []Value{}
		r.Metadata.ExecId = 1
		val, err := r.CreateExecString(arglist)
		if err != nil {
			t.Error(err)
		}
		if val != "EXEC 1 ()" {
			t.Error("Function did not return expexted value")
		}
	})

}

func TestResultSetStoreResult(t *testing.T) {
	t.Run("Verify StoreResult with empty result", func(t *testing.T) {
		var r ResultSet
		var response = ""
		err := r.StoreResult(response)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Verify StoreResult from create table", func(t *testing.T) {
		var r ResultSet
		var response = `&5 0 0 6 0 0 0 0 20
% .prepare,     .prepare,       .prepare,       .prepare,       .prepare,       .prepare # table_name
% type, digits, scale,  schema, table,  column # name
% varchar,      int,    int,    varchar,        varchar,        varchar # type
% 0,    1,      1,      0,      0,      0 # length
% 0 0,  1 0,    1 0,    0 0,    0 0,    0 0 # typesizes
	
&3 128 127
			
`
		err := r.StoreResult(response)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Verify StoreResult from prepare select star", func(t *testing.T) {
		var r ResultSet
		var response = `&5 2 1 6 1 0 0 0 36
% .prepare,     .prepare,       .prepare,       .prepare,       .prepare,       .prepare # table_name
% type, digits, scale,  schema, table,  column # name
% varchar,      int,    int,    varchar,        varchar,        varchar # type
% 7,    2,      1,      0,      5,      4 # length
% 7 0,  2 0,    1 0,    0 0,    0 0,    4 0 # typesizes
[ "varchar",    16,     0,      "",     "test1",        "name"  ]
		
`
		err := r.StoreResult(response)
		if err != nil {
			t.Error(err)
		}
		//if r.Schema[0].DisplaySize != 5 {
		//	t.Error("unexpected displaysize")
		//}
		//if r.Schema[0].InternalSize != 16 {
		//	t.Error("Unexpected internalsize")
		//}
	})

	t.Run("Verify StoreResult from prepare select star", func(t *testing.T) {
		var r ResultSet
		var response = `&1 2 1 1 1 0 201 169 7
% sys.test1 # table_name
% name # name
% varchar # type
% 5 # length
% 16 0 # typesizes
[ "name1"       ]
		
`
		err := r.StoreResult(response)
		if err != nil {
			t.Error(err)
		}
		if r.Schema[0].DisplaySize != 5 {
			t.Error("unexpected displaysize")
		}
		if r.Schema[0].InternalSize != 16 {
			t.Error("Unexpected internalsize")
		}
	})

}
