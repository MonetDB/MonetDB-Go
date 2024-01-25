/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"bytes"
	"testing"
	"time"
)

func TestConvertToMonet(t *testing.T) {
	type tc struct {
		v Value
		e string
	}
	var tcs = []tc{
		{1, "1"},
		{"string", "'string'"},
		{"quoted 'string'", "'quoted \\'string\\''"},
		{"quoted \"string\"", "'quoted \"string\"'"},
		{"back\\slashed", "'back\\\\slashed'"},
		{"quoted \\'string\\'", "'quoted \\\\\\'string\\\\\\''"},
		{int8(8), "8"},
		{int16(16), "16"},
		{int32(32), "32"},
		{int64(64), "64"},
		{float32(3.2), "3.2"},
		{float64(6.4), "6.4"},
		{true, "true"},
		{false, "false"},
		{nil, "NULL"},
		{[]byte{1, 2, 3}, "'" + string([]byte{1, 2, 3}) + "'"},
		{Time{10, 20, 30}, "'10:20:30'"},
		{Date{2001, time.January, 2}, "'2001-01-02'"},
		{time.Date(2001, time.January, 2, 10, 20, 30, 0, time.FixedZone("CET", 3600)),
			"'2001-01-02 10:20:30 +0100 CET'"},
	}

	for _, c := range tcs {
		s, err := ConvertToMonet(c.v)
		if err != nil {
			t.Errorf("Error converting value: %v -> %v", c.v, err)
		} else if s != c.e {
			t.Errorf("Invalid value: %s, expected: %s", s, c.e)
		}
	}
}

func TestConvertToGo(t *testing.T) {
	type tc struct {
		v string
		t string
		e Value
	}
	var tcs = []tc{
		{"8", "tinyint", int8(8)},
		{"16", "smallint", int16(16)},
		{"16", "shortint", int16(16)},
		{"32", "int", int32(32)},
		{"32", "mediumint", int32(32)},
		{"64", "bigint", int64(64)},
		{"64", "longint", int64(64)},
		{"64", "hugeint", int64(64)},
		{"64", "serial", int64(64)},
		{"3.2", "float", float32(3.2)},
		{"3.2", "real", float32(3.2)},
		{"6.4", "double", float64(6.4)},
		{"6.4", "decimal", float64(6.4)},
		{"true", "boolean", true},
		{"false", "boolean", false},
		{"10:20:30", "time", Time{10, 20, 30}},
		{"2001-01-02", "date", Date{2001, time.January, 2}},
		{"'string'", "char", "string"},
		{"'string'", "varchar", "string"},
		{"'quoted \"string\"'", "char", "quoted \"string\""},
		{"'quoted \\'string\\''", "char", "quoted 'string'"},
		{"'quoted \\\\\\'string\\\\\\''", "char", "quoted \\'string\\'"},
		{"'back\\\\slashed'", "char", "back\\slashed"},
		{"'ABC'", "blob", []uint8{0x41, 0x42, 0x43}},
	}

	for _, c := range tcs {
		v, err := convertToGo(c.v, c.t)
		if err != nil {
			t.Errorf("Error converting value: %v (%s) -> %v", c.v, c.t, err)
		} else {
			ok := true
			switch val := v.(type) {
			case []byte:
				ok = compareByteArray(t, val, c.e)
			default:
				ok = v == c.e
			}
			if !ok {
				t.Errorf("Invalid value: %v (%v - %s), expected: %v", v, c.v, c.t, c.e)
			}
		}
	}
}

func compareByteArray(t *testing.T, val []byte, e Value) bool {
	switch exp := e.(type) {
	case []byte:
		return bytes.Equal(val, exp)
	default:
		return false
	}
}
