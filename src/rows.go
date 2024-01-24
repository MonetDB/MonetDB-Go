/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"strings"
	"reflect"
	"time"
	"context"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Rows struct {
	conn        *mapi.MapiConn
	resultset   *mapi.ResultSet
	active      bool
	queryId     int
	err         error

	rowNum      int
	offset      int
	lastRowId   int
	rowCount    int
	rows        [][]driver.Value
	schema      []mapi.TableElement
	columns     []string
}

func newRows(c *mapi.MapiConn, r *mapi.ResultSet) *Rows {
	return &Rows{
		conn:      c,
		resultset: r,
		active:    true,
		err:       nil,

		columns:   nil,
		rowNum:    0,
	}
}

func (r *Rows) Columns() []string {
	if r.columns == nil {
		r.columns = make([]string, len(r.schema))
		for i, d := range r.schema {
			r.columns[i] = d.ColumnName
		}
	}
	return r.columns
}

func (r *Rows) Close() error {
	r.active = false
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if !r.active {
		return fmt.Errorf("monetdb: rows closed")
	}
	if r.queryId == -1 {
		return fmt.Errorf("monetdb: query didn't result in a resultset")
	}

	if r.rowNum >= r.rowCount {
		return io.EOF
	}

	if r.rowNum >= r.offset+len(r.rows) {
		err := r.fetchNext()
		if err != nil {
			return err
		}
	}

	for i, v := range r.rows[r.rowNum-r.offset] {
		if vv, ok := v.(string); ok {
			dest[i] = []byte(vv)
		} else {
			dest[i] = v
		}
	}
	r.rowNum += 1

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

// This function call to FetchNext connects to the database and can potentially take a long time. Therefore
// we want to be able to cancel it, so we run it inside a goroutine.
func (s *Rows) mapiDo(ctx context.Context, amount int) (string, error) {
	type res struct {
		resultstring string;
		err error
	}
	c := make(chan res, 1)

    go func() {
		r, err := s.conn.FetchNext(s.queryId, s.offset, amount)
		result := res{r, err}
		c <- result
		}()

    select {
    case <-ctx.Done():
        <-c // Wait for the goroutine to return. Later we need to cancel the query on the database
        return "", ctx.Err()
    case result := <-c:
        return result.resultstring, result.err
    }
}

func (r *Rows) fetchNext() error {
	if r.rowNum >= r.rowCount {
		return io.EOF
	}

	r.offset += len(r.rows)
	end := min(r.rowCount, r.rowNum+mapi.MAPI_ARRAY_SIZE)
	amount := end - r.offset

	res, err := r.mapiDo(context.Background(), amount)
	if err != nil {
		return err
	}

	r.resultset.StoreResult(res)
	r.rows = convertRows(r.resultset.Rows, r.resultset.Metadata.ColumnCount)
	r.schema = r.resultset.Schema

	return nil
}

// See https://pkg.go.dev/database/sql/driver#RowsColumnTypeLength for what to implement
// This implies that we need to return the InternalSize value, not the DisplaySize
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	switch r.schema[index].ColumnType {
	case mapi.MDB_VARCHAR,
		mapi.MDB_CHAR :
		return int64(r.schema[index].InternalSize), true
	case mapi.MDB_BLOB,
		mapi.MDB_CLOB :
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

// See https://pkg.go.dev/database/sql/driver#RowsColumnTypeDatabaseTypeName for what to implement
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper(r.schema[index].ColumnType)
}

// For now it seems that the mapi protocol does not provide the required information
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}

// See https://pkg.go.dev/database/sql/driver#RowsColumnTypePrecisionScale for what to implement
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	switch r.schema[index].ColumnType {
	case mapi.MDB_DECIMAL :
		return int64(r.schema[index].Precision), int64(r.schema[index].Scale), true
	default:
		return 0, 0, false
	}
}

// See https://pkg.go.dev/database/sql/driver#RowsColumnTypeScanType for what to implement
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	var scantype reflect.Type

	switch r.schema[index].ColumnType {
	case mapi.MDB_VARCHAR,
		mapi.MDB_CHAR,
		mapi.MDB_CLOB,
		mapi.MDB_INTERVAL,
		mapi.MDB_MONTH_INTERVAL,
		mapi.MDB_SEC_INTERVAL :
		scantype = reflect.TypeOf("")
	case mapi.MDB_NULL :
		scantype = reflect.TypeOf(nil)
	case mapi.MDB_BLOB :
		scantype = reflect.TypeOf([]uint8{0})
	case mapi.MDB_BOOLEAN :
		scantype = reflect.TypeOf(true)
	case mapi.MDB_REAL,
		mapi.MDB_FLOAT :
		scantype = reflect.TypeOf(float32(0))
	case mapi.MDB_DECIMAL,
		mapi.MDB_DOUBLE :
		scantype = reflect.TypeOf(float64(0))
	case mapi.MDB_TINYINT :
		scantype = reflect.TypeOf(int8(0))
	case mapi.MDB_SHORTINT,
		mapi.MDB_SMALLINT :
		scantype = reflect.TypeOf(int16(0))
	case mapi.MDB_INT,
		mapi.MDB_MEDIUMINT,
		mapi.MDB_WRD :
		scantype = reflect.TypeOf(int32(0))
	case mapi.MDB_BIGINT,
		mapi.MDB_HUGEINT,
		mapi.MDB_SERIAL,
		mapi.MDB_LONGINT :
		scantype = reflect.TypeOf(int64(0))
	case mapi.MDB_DATE,
		mapi.MDB_TIME,
		mapi.MDB_TIMESTAMP,
		mapi.MDB_TIMESTAMPTZ :
		scantype = reflect.TypeOf(time.Time{})
	default:
		scantype = reflect.TypeOf(nil)
	}
	return scantype
}
