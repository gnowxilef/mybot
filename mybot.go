/*

mybot - Illustrative Slack bot in Go

Copyright (c) 2015 RapidLoop

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/patrickmn/go-cache"
	"golang.org/x/net/websocket"
	"log"
	"os"
	"strings"
	"time"
)

type Configuration struct {
	cassandra string
	cassandraKeyspace string
	slackToken string
}

func parseCommandLine() (error, *Configuration) {
	var result Configuration

	flag.StringVar( &result.cassandra, "cassandra", "8.14.147.250", "A node in the cassandra cluster to attach to" )
	flag.StringVar( &result.cassandraKeyspace, "cassandra-keyspace", "sailbot", "Cassandra keyspace to connect to" )
	flag.StringVar( &result.slackToken, "slack-token", "", "slack token for this bot" )
	flag.Parse()

	if flag.NArg() != 0 {
		flag.Usage()
		return errors.New("Unknown arguments"), nil
	}
	if result.slackToken == "" {
		return errors.New("slack-token is required"), nil
	}

	return nil, &result
}

func main() {
	var cmdError, config = parseCommandLine()
	if cmdError != nil {
		fmt.Fprintf( os.Stderr, "Error configuring: %s\n", cmdError.Error() )
		os.Exit(-1)
	}

	c := cache.New(3600*time.Minute, 30*time.Second)

	session := startup( config.cassandra, config.cassandraKeyspace )
	defer session.Close()
	answers := lookup(session, "what's an anchor chain?")
	if len(answers) != 1 {
		fmt.Printf("%q", answers)
		os.Exit(1)
	}

	// start a websocket-based Real Time API session
	ws, id := slackConnect( config.slackToken )
	fmt.Printf("mybot ready, id = %s, ^C exits\n", id)

	ticker := time.NewTicker(time.Hour * 24)
	go func(session *gocql.Session, ws *websocket.Conn) {
		var m Message
		m.Type = "message"
		m.Channel = "C03N4M0LK"
		for _ = range ticker.C {
			ans := getRandom(session)
			for _, def := range ans {
				if _, found := c.Get(def[0]); found {
					continue
				}
				if len(def[1]) > 0 {
					m.Text = "*" + def[0] + " " + def[1] + "*: " + def[2]
				} else {
					m.Text = "*" + def[0] + "*: " + def[2]
				}
				postMessage(ws, m)
				fmt.Printf("send %s\n", m.Text)
				c.Set(def[0], 0, cache.DefaultExpiration)
			}
		}
	}(session, ws)

	for {
		// read each incoming message
		m, err := getMessage(ws)
		if err != nil {
			log.Fatal(err)
		}

		if m.Type == "message" {
			// if so try to parse if
			ans := lookup(session, m.Text)
			if len(ans) > 0 {
				// looks good, get the quote and reply with the result
				go func(m Message) {
					for _, def := range ans {
						if _, found := c.Get(def[0]); found {
							continue
						}
						if len(def[1]) > 0 {
							m.Text = "*" + def[0] + " " + def[1] + "*: " + def[2]
						} else {
							m.Text = "*" + def[0] + "*: " + def[2]
						}
						postMessage(ws, m)
						c.Set(def[0], 0, cache.DefaultExpiration)
					}
				}(m)
				// NOTE: the Message object is copied, this is intentional
            defn := strings.Split(m.Text, " := ")
            if len(defn) > 1 {
                go func(m Message) {
                    addDefn(session, defn[0], defn[1])
                    m.Text = "Ack."
                    postMessage(ws, m)
                }(m)
            }
	    }
		}
	}
}
