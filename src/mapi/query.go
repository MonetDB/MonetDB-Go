/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
*/

package mapi

import (
	"fmt"
)

type Query struct {
	Mapi   *MapiConn
	SqlQuery string
}

func (q *Query) execute(query string) (string, error) {
	if q.Mapi == nil {
		return "", fmt.Errorf("monetdb: database connection is closed")
	}
	return q.Mapi.Execute(query)
}

func (q *Query) PrepareQuery(r *ResultSet) error {
	querystring := fmt.Sprintf("PREPARE %s", q.SqlQuery)
	resultstring, err := q.execute(querystring)

	if err != nil {
		return err
	}
	return r.StoreResult(resultstring)
}

func (q *Query) ExecutePreparedQuery(r *ResultSet, args []Value) (string, error) {
	execStr, err := r.CreateExecString(args)
	if err != nil {
		return "", err
	} 
	return q.execute(execStr)
}

func (q *Query) ExecuteNamedQuery(r *ResultSet, names []string, args []Value) (string, error) {
	execStr, err := r.CreateNamedString(q.SqlQuery, names, args)
	if err != nil {
		return "", err
	}
	return q.execute(execStr)
}

func (q *Query) ExecuteQuery(r *ResultSet) (string, error) {
	return q.execute(q.SqlQuery)
}

