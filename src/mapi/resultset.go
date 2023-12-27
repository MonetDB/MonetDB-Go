/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type TableElement struct {
	ColumnName   string
	ColumnType   string
	DisplaySize  int
	InternalSize int
	Precision    int
	Scale        int
	NullOk       int
}

type Metadata struct {
	ExecId      int
	LastRowId   int
	RowCount    int
	QueryId     int
	Offset      int
	ColumnCount int
}

type Value interface{}

type ResultSet struct {
	Metadata Metadata
	Schema []TableElement
	Rows [][]Value
}

func (s *ResultSet) StoreResult(r string) error {
	var columnNames []string
	var columnTypes []string
	var displaySizes []int
	var internalSizes []int
	var precisions []int
	var scales []int
	var nullOks []int

	for _, line := range strings.Split(r, "\n") {
		if strings.HasPrefix(line, mapi_MSG_INFO) {
			// TODO log

		} else if strings.HasPrefix(line, mapi_MSG_QPREPARE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			s.Metadata.ExecId, _ = strconv.Atoi(t[0])
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_QTABLE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			s.Metadata.QueryId, _ = strconv.Atoi(t[0])
			s.Metadata.RowCount, _ = strconv.Atoi(t[1])
			s.Metadata.ColumnCount, _ = strconv.Atoi(t[2])

			columnNames = make([]string, s.Metadata.ColumnCount)
			columnTypes = make([]string, s.Metadata.ColumnCount)
			displaySizes = make([]int, s.Metadata.ColumnCount)
			internalSizes = make([]int, s.Metadata.ColumnCount)
			precisions = make([]int, s.Metadata.ColumnCount)
			scales = make([]int, s.Metadata.ColumnCount)
			nullOks = make([]int, s.Metadata.ColumnCount)

		} else if strings.HasPrefix(line, mapi_MSG_TUPLE) {
			v, err := s.parseTuple(line)
			if err != nil {
				return err
			}
			s.Rows = append(s.Rows, v)

		} else if strings.HasPrefix(line, mapi_MSG_QBLOCK) {
			s.Rows = make([][]Value, 0)

		} else if strings.HasPrefix(line, mapi_MSG_QSCHEMA) {
			s.Metadata.Offset = 0
			s.Rows = make([][]Value, 0)
			s.Metadata.LastRowId = 0
			s.Schema = nil
			s.Metadata.RowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_QUPDATE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			s.Metadata.RowCount, _ = strconv.Atoi(t[0])
			s.Metadata.LastRowId, _ = strconv.Atoi(t[1])

		} else if strings.HasPrefix(line, mapi_MSG_QTRANS) {
			s.Metadata.Offset = 0
			s.Rows = make([][]Value, 0)
			s.Metadata.LastRowId = 0
			s.Schema = nil
			s.Metadata.RowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_HEADER) {
			t := strings.Split(line[1:], "#")
			data := strings.TrimSpace(t[0])
			identity := strings.TrimSpace(t[1])

			values := make([]string, 0)
			for _, value := range strings.Split(data, ",") {
				values = append(values, strings.TrimSpace(value))
			}

			if identity == "name" {
				columnNames = values

			} else if identity == "type" {
				columnTypes = values

			} else if identity == "typesizes" {
				sizes := make([][]int, len(values))
				for i, value := range values {
					s := make([]int, 0)
					for _, v := range strings.Split(value, " ") {
						val, _ := strconv.Atoi(v)
						s = append(s, val)
					}
					internalSizes[i] = s[0]
					sizes[i] = s
				}
				for j, t := range columnTypes {
					if t == "decimal" {
						precisions[j] = sizes[j][0]
						scales[j] = sizes[j][1]
					}
				}
			} else if identity == "length" {
				for i, value := range values {
					s := make([]int, 0)
					for _, v := range strings.Split(value, " ") {
						val, _ := strconv.Atoi(v)
						s = append(s, val)
					}
					displaySizes[i] = s[0]
				}
			}

			s.updateSchema(columnNames, columnTypes, displaySizes,
				internalSizes, precisions, scales, nullOks)
			s.Metadata.Offset = 0
			s.Metadata.LastRowId = 0

		} else if strings.HasPrefix(line, mapi_MSG_PROMPT) {
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_ERROR) {
			return fmt.Errorf("mapi: database error: %s", line[1:])
		}
	}

	return fmt.Errorf("mapi: unknown state: %s", r)
}

func (s *ResultSet) parseTuple(d string) ([]Value, error) {
	items := strings.Split(d[1:len(d)-1], ",\t")
	if len(items) != len(s.Schema) {
		return nil, fmt.Errorf("mapi: length of row doesn't match header")
	}

	v := make([]Value, len(items))
	for i, value := range items {
		vv, err := s.convert(value, s.Schema[i].ColumnType)
		if err != nil {
			return nil, err
		}
		v[i] = vv
	}
	return v, nil
}

func (s *ResultSet) updateSchema(
	columnNames, columnTypes []string, displaySizes,
	internalSizes, precisions, scales, nullOks []int) {

	d := make([]TableElement, len(columnNames))
	for i, columnName := range columnNames {
		desc := TableElement{
			ColumnName:   columnName,
			ColumnType:   columnTypes[i],
			DisplaySize:  displaySizes[i],
			InternalSize: internalSizes[i],
			Precision:    precisions[i],
			Scale:        scales[i],
			NullOk:       nullOks[i],
		}
		d[i] = desc
	}

	s.Schema = d
}

func (s *ResultSet) convert(value, dataType string) (Value, error) {
	val, err := convertToGo(value, dataType)
	return val, err
}

func (s *ResultSet) CreateExecString(args []Value) (string, error) {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("EXEC %d (", s.Metadata.ExecId))

	for i, v := range args {
		str, err := ConvertToMonet(v)
		if err != nil {
			return "", nil
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(str)
	}

	b.WriteString(")")
	return b.String(), nil
}