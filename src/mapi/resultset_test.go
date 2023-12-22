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
