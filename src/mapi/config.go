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

type Config struct {
	Username string
	Password string
	Hostname string
	Database string
	Port     int
}

func ParseDSN(name string) (Config, error) {
	ipv6_re := regexp.MustCompile(`^((?P<username>[^:]+?)(:(?P<password>[^@]+?))?@)?\[(?P<hostname>(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))+?)\](:(?P<port>\d+?))?\/(?P<database>.+?)$`)

	if ipv6_re.MatchString(name) {
		//lint:ignore SA4006 prepare to enable staticchecks
		m := make([]string, 0)
		//lint:ignore SA4006 prepare to enable staticchecks
		n := make([]string, 0)
		m = ipv6_re.FindAllStringSubmatch(name, -1)[0]
		n = ipv6_re.SubexpNames()
		return getConfig(m, n, true), nil
	}

	c := Config{
		Hostname: "localhost",
		Port:     50000,
	}

	reversed := reverse(name)

	host, creds, _ := Cut(reversed, "@") // host, creds, found

	configWithHost, err := parseHost(reverse(host), c)

	if err != nil {
		//lint:ignore ST1005 prepare to enable staticchecks
		return Config{}, fmt.Errorf("Invalid DSN")
	}

	newConfig, err := parseCreds(reverse(creds), configWithHost)

	if err != nil {
		//lint:ignore ST1005 prepare to enable staticchecks
		return Config{}, fmt.Errorf("Invalid DSN")
	}

	return newConfig, nil
}

func parseCreds(creds string, c Config) (Config, error) {
	username, password, found := Cut(creds, ":")

	c.Username = username
	c.Password = ""

	if found {
		if username == "" {
			//lint:ignore ST1005 prepare to enable staticchecks
			return c, fmt.Errorf("Invalid DSN")
		}

		c.Password = password
	}

	return c, nil
}

func parseHost(host string, c Config) (Config, error) {
	host, dbName, found := Cut(host, "/")

	if !found {
		//lint:ignore ST1005 prepare to enable staticchecks
		return c, fmt.Errorf("Invalid DSN")
	}

	if host == "" {
		//lint:ignore ST1005 prepare to enable staticchecks
		return c, fmt.Errorf("Invalid DSN")
	}

	c.Database = dbName

	hostname, port, found := Cut(host, ":")

	if !found {
		return c, nil
	}

	c.Hostname = hostname

	port_num, err := strconv.Atoi(port)

	if err != nil {
		//lint:ignore ST1005 prepare to enable staticchecks
		return c, fmt.Errorf("Invalid DSN")
	}

	c.Port = port_num

	return c, nil
}

func getConfig(m []string, n []string, ipv6 bool) Config {
	c := Config{
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
