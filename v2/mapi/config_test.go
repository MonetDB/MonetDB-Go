/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"strconv"
	"testing"
)

func TestParseDSN(t *testing.T) {
	tcs := [][]string{
		{"me:secret@localhost:1234/testdb", "me", "secret", "localhost", "1234", "testdb"},
		{"me@localhost:1234/testdb", "me", "", "localhost", "1234", "testdb"},
		{"localhost:1234/testdb", "", "", "localhost", "1234", "testdb"},
		{"user:P@sswordWith@@localhost:50000/db", "user", "P@sswordWith@", "localhost", "50000", "db"},
		{"localhost/testdb", "", "", "localhost", "50000", "testdb"},
		{"localhost"},
		{"/testdb"},
		{"/"},
		{""},
		{":secret@localhost:1234/testdb"},
	}

	for _, tc := range tcs {
		n := tc[0]
		ok := len(tc) > 1
		c, err := parseDSN(n)

		if ok && err != nil {
			t.Errorf("Error parsing DSN: %s -> %v", n, err)
		} else if !ok && err == nil {
			t.Errorf("Error parsing invalid DSN: %s", n)
		}

		if !ok || err != nil {
			continue
		}

		port, _ := strconv.Atoi(tc[4])

		if c.Username != tc[1] {
			t.Errorf("Invalid username: %s, expected: %s", c.Username, tc[1])
		}
		if c.Password != tc[2] {
			t.Errorf("Invalid password: %s, expected: %s", c.Password, tc[2])
		}
		if c.Hostname != tc[3] {
			t.Errorf("Invalid hostname: %s, expected: %s", c.Hostname, tc[3])
		}
		if c.Port != port {
			t.Errorf("Invalid port: %d, expected: %d", c.Port, port)
		}
		if c.Database != tc[5] {
			t.Errorf("Invalid database: %s, expected: %s", c.Database, tc[5])
		}
	}

}
func TestParseIpv6DSN(t *testing.T) {
	tcs := [][]string{
		{"me:secret@[::1]:1234/testdb", "me", "secret", "[::1]", "1234", "testdb"},
		{"me:secret@[1:2:3:4:5:6:7:8]:1234/testdb", "me", "secret", "[1:2:3:4:5:6:7:8]", "1234", "testdb"},
	}

	for _, tc := range tcs {
		n := tc[0]
		ok := len(tc) > 1
		c, err := parseDSN(n)

		if ok && err != nil {
			t.Errorf("Error parsing DSN: %s -> %v", n, err)
		} else if !ok && err == nil {
			t.Errorf("Error parsing invalid DSN: %s", n)
		}

		if !ok || err != nil {
			continue
		}

		port, _ := strconv.Atoi(tc[4])

		if c.Username != tc[1] {
			t.Errorf("Invalid username: %s, expected: %s", c.Username, tc[1])
		}
		if c.Password != tc[2] {
			t.Errorf("Invalid password: %s, expected: %s", c.Password, tc[2])
		}
		if c.Hostname != tc[3] {
			t.Errorf("Invalid hostname: %s, expected: %s", c.Hostname, tc[3])
		}
		if c.Port != port {
			t.Errorf("Invalid port: %d, expected: %d", c.Port, port)
		}
		if c.Database != tc[5] {
			t.Errorf("Invalid database: %s, expected: %s", c.Database, tc[5])
		}
	}

}
