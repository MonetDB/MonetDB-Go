/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type config struct {
	Username string
	Password string
	Hostname string
	Database string
	Port     int
}

func parseDSN(name string) (config, error) {
	ipv6_re := regexp.MustCompile(`^((?P<username>[^:]+?)(:(?P<password>[^@]+?))?@)?\[(?P<hostname>(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))+?)\](:(?P<port>\d+?))?\/(?P<database>.+?)$`)

	if ipv6_re.MatchString(name) {
		m := ipv6_re.FindAllStringSubmatch(name, -1)[0]
		n := ipv6_re.SubexpNames()
		return getConfig(m, n, true), nil
	}

	c := config{
		Hostname: "localhost",
		Port:     50000,
	}

	reversed := reverse(name)

	host, creds, _ := Cut(reversed, "@") // host, creds, found

	configWithHost, err := parseHost(reverse(host), c)

	if err != nil {
		return config{}, fmt.Errorf("mapi: invalid DSN")
	}

	newConfig, err := parseCreds(reverse(creds), configWithHost)

	if err != nil {
		return config{}, fmt.Errorf("mapi: invalid DSN")
	}

	return newConfig, nil
}

func parseCreds(creds string, c config) (config, error) {
	username, password, found := Cut(creds, ":")

	c.Username = username
	c.Password = ""

	if found {
		if username == "" {
			return c, fmt.Errorf("mapi: invalid DSN")
		}

		c.Password = password
	}

	return c, nil
}

func parseHost(host string, c config) (config, error) {
	host, dbName, found := Cut(host, "/")

	if !found {
		return c, fmt.Errorf("mapi: invalid DSN")
	}

	if host == "" {
		return c, fmt.Errorf("mapi: invalid DSN")
	}

	c.Database = dbName

	hostname, port, found := Cut(host, ":")

	if !found {
		return c, nil
	}

	c.Hostname = hostname

	port_num, err := strconv.Atoi(port)

	if err != nil {
		return c, fmt.Errorf("mapi: invalid DSN")
	}

	c.Port = port_num

	return c, nil
}

func getConfig(m []string, n []string, ipv6 bool) config {
	c := config{
		Hostname: "localhost",
		Port:     50000,
	}
	for i, v := range m {
		if n[i] == "username" {
			c.Username = v
		} else if n[i] == "password" {
			c.Password = v
		} else if n[i] == "hostname" {
			if ipv6 {
				c.Hostname = fmt.Sprintf("[%s]", v)
				continue
			}

			c.Hostname = v
		} else if n[i] == "port" && v != "" {
			c.Port, _ = strconv.Atoi(v)
		} else if n[i] == "database" {
			c.Database = v
		}
	}

	return c
}

func reverse(in string) string {
	var sb strings.Builder
	runes := []rune(in)
	for i := len(runes) - 1; 0 <= i; i-- {
		sb.WriteRune(runes[i])
	}
	return sb.String()
}

func Cut(s, sep string) (before, after string, found bool) {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}
