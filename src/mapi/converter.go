/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MDB_CHAR      = "char"    // (L) character string with length L
	MDB_VARCHAR   = "varchar" // (L) string with atmost length L
	MDB_CLOB      = "clob"
	MDB_BLOB      = "blob"
	MDB_DECIMAL   = "decimal"  // (P,S)
	MDB_SMALLINT  = "smallint" // 16 bit integer
	MDB_INT       = "int"      // 32 bit integer
	MDB_BIGINT    = "bigint"   // 64 bit integer
	MDB_HUGEINT   = "hugeint"  // 64 bit integer
	MDB_SERIAL    = "serial"   // special 64 bit integer sequence generator
	MDB_REAL      = "real"     // 32 bit floating point
	MDB_DOUBLE    = "double"   // 64 bit floating point
	MDB_BOOLEAN   = "boolean"
	MDB_DATE      = "date"
	MDB_NULL      = "NULL"
	MDB_TIME      = "time"      // (T) time of day
	MDB_TIMESTAMP = "timestamp" // (T) date concatenated with unique time
	MDB_INTERVAL  = "interval"  // (Q) a temporal interval

	MDB_MONTH_INTERVAL = "month_interval"
	MDB_SEC_INTERVAL   = "sec_interval"
	MDB_WRD            = "wrd"
	MDB_TINYINT        = "tinyint"

	// Not on the website:
	MDB_SHORTINT    = "shortint"
	MDB_MEDIUMINT   = "mediumint"
	MDB_LONGINT     = "longint"
	MDB_FLOAT       = "float"
	MDB_TIMESTAMPTZ = "timestamptz"

	// full names and aliases, spaces are replaced with underscores
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_CHARACTER               = MDB_CHAR
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_CHARACTER_VARYING       = MDB_VARCHAR
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_CHARACHTER_LARGE_OBJECT = MDB_CLOB
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_BINARY_LARGE_OBJECT     = MDB_BLOB
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_NUMERIC                 = MDB_DECIMAL
	//lint:ignore U1000 prepare to enable staticchecks
	mdb_DOUBLE_PRECISION        = MDB_DOUBLE
)

var timeFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05 -0700 MST",
	"Mon Jan 2 15:04:05 -0700 MST 2006",
	"2006-01-02 15:04:05.999999+00:00",
	"15:04:05",
}

type toGoConverter func(string) (Value, error)
type toMonetConverter func(Value) (string, error)

func strip(v string) (Value, error) {
	return unquote(strings.TrimSpace(v[1 : len(v)-1]))
}

// from strconv.contains
// contains reports whether the string contains the byte c.
func contains(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}

// adapted from strconv.Unquote
func unquote(s string) (string, error) {
	// Is it trivial?  Avoid allocation.
	if !contains(s, '\\') {
		return s, nil
	}

	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := strconv.UnquoteChar(s, '\'')
		if err != nil {
			fmt.Printf("E: %v\n -> %s\n", err, s)
			return "", err
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}

func toByteArray(v string) (Value, error) {
	return []byte(v[1 : len(v)-1]), nil
}

func toDouble(v string) (Value, error) {
	return strconv.ParseFloat(v, 64)
}

func toFloat(v string) (Value, error) {
	var r float32
	i, err := strconv.ParseFloat(v, 32)
	if err == nil {
		r = float32(i)
	}
	return r, err
}

func toInt8(v string) (Value, error) {
	var r int8
	i, err := strconv.ParseInt(v, 10, 8)
	if err == nil {
		r = int8(i)
	}
	return r, err
}

func toInt16(v string) (Value, error) {
	var r int16
	i, err := strconv.ParseInt(v, 10, 16)
	if err == nil {
		r = int16(i)
	}
	return r, err
}

func toInt32(v string) (Value, error) {
	var r int32
	i, err := strconv.ParseInt(v, 10, 32)
	if err == nil {
		r = int32(i)
	}

	return r, err
}

func toInt64(v string) (Value, error) {
	var r int64
	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		r = int64(i)
	}

	return r, err
}

func parseTime(v string) (t time.Time, err error) {
	for _, f := range timeFormats {
		t, err = time.Parse(f, v)
		if err == nil {
			return
		}
	}
	return
}

func toNil(v string) (Value, error) {
	return "NULL", nil
}

func toBool(v string) (Value, error) {
	return strconv.ParseBool(v)
}

func toDate(v string) (Value, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	year, month, day := t.Date()
	return Date{year, month, day}, nil
}

func toTime(v string) (Value, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	hour, min, sec := t.Clock()
	return Time{hour, min, sec}, nil
}
func toTimestamp(v string) (Value, error) {
	return parseTime(v)
}
func toTimestampTz(v string) (Value, error) {
	return parseTime(v)
}

var toGoMappers = map[string]toGoConverter{
	MDB_CHAR:           strip,
	MDB_VARCHAR:        strip,
	MDB_CLOB:           strip,
	MDB_BLOB:           toByteArray,
	MDB_DECIMAL:        toDouble,
	MDB_NULL:           toNil,
	MDB_SMALLINT:       toInt16,
	MDB_INT:            toInt32,
	MDB_WRD:            toInt32,
	MDB_BIGINT:         toInt64,
	MDB_HUGEINT:        toInt64,
	MDB_SERIAL:         toInt64,
	MDB_REAL:           toFloat,
	MDB_DOUBLE:         toDouble,
	MDB_BOOLEAN:        toBool,
	MDB_DATE:           toDate,
	MDB_TIME:           toTime,
	MDB_TIMESTAMP:      toTimestamp,
	MDB_TIMESTAMPTZ:    toTimestampTz,
	MDB_INTERVAL:       strip,
	MDB_MONTH_INTERVAL: strip,
	MDB_SEC_INTERVAL:   strip,
	MDB_TINYINT:        toInt8,
	MDB_SHORTINT:       toInt16,
	MDB_MEDIUMINT:      toInt32,
	MDB_LONGINT:        toInt64,
	MDB_FLOAT:          toFloat,
}

func toString(v Value) (string, error) {
	return fmt.Sprintf("%v", v), nil
}

func toQuotedString(v Value) (string, error) {
	s := fmt.Sprintf("%v", v)
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	return fmt.Sprintf("'%v'", s), nil
}

func toNull(v Value) (string, error) {
	return "NULL", nil
}

func toByteString(v Value) (string, error) {
	switch val := v.(type) {
	case []uint8:
		return toQuotedString(string(val))
	default:
		return "", fmt.Errorf("unsupported type")
	}
}

func toDateTimeString(v Value) (string, error) {
	switch val := v.(type) {
	case Time:
		return toQuotedString(fmt.Sprintf("%02d:%02d:%02d", val.Hour, val.Min, val.Sec))
	case Date:
		return toQuotedString(fmt.Sprintf("%04d-%02d-%02d", val.Year, val.Month, val.Day))
	default:
		//lint:ignore ST1005 prepare to enable staticchecks
		return "", fmt.Errorf("Unsupported type")
	}
}

var toMonetMappers = map[string]toMonetConverter{
	"int":          toString,
	"int8":         toString,
	"int16":        toString,
	"int32":        toString,
	"int64":        toString,
	"float":        toString,
	"float32":      toString,
	"float64":      toString,
	"bool":         toString,
	"string":       toQuotedString,
	"nil":          toNull,
	"null":         toNull,
	"[]uint8":      toByteString,
	"time.Time":    toQuotedString,
	"mapi.Time": toDateTimeString,
	"mapi.Date": toDateTimeString,
}

func convertToGo(value, dataType string) (Value, error) {
	if strings.TrimSpace(value) == "NULL" {
		dataType = "NULL"
	}

	if mapper, ok := toGoMappers[dataType]; ok {
		value := strings.TrimSpace(value)
		return mapper(value)
	}
	//lint:ignore ST1005 prepare to enable staticchecks
	return nil, fmt.Errorf("Type not supported: %s", dataType)
}

func ConvertToMonet(value Value) (string, error) {
	t := reflect.TypeOf(value)
	n := "nil"
	if t != nil {
		n = t.String()
	}

	if mapper, ok := toMonetMappers[n]; ok {
		return mapper(value)
	}
	//lint:ignore ST1005 prepare to enable staticchecks
	return "", fmt.Errorf("Type not supported: %v", t)
}
