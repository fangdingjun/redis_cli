package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

//
// redis connection protocol
//  https://redis.io/topics/protocol
//

func encodeCmd(args ...string) string {
	// command send with array
	cmd := "*"
	cmd = fmt.Sprintf("%s%d\r\n", cmd, len(args))
	for _, s1 := range args {
		cmd = fmt.Sprintf("%s$%d\r\n%s\r\n", cmd, len(s1), s1)
	}
	return cmd
}

func readResp(r io.Reader) {
	var narr int
	var nindex int
	reader := bufio.NewReader(r)
	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			//fmt.Println(err)
			break
		}
		if narr > 0 {
			fmt.Printf("%d) ", nindex)
		}
		switch s[0] {
		case '+': // simple string
			fmt.Printf("\"%s\"\n", s[1:len(s)-2])
		case '-': // error
			fmt.Printf("%s\n", s[1:len(s)-2])
		case ':': // integer
			fmt.Println(s[1 : len(s)-2])
		case '$': // bulk string
			l := s[1 : len(s)-2]
			l1, err := strconv.Atoi(l)
			if err != nil {
				fmt.Println("invalid message format", err)
				break
			}
			if l1 == -1 {
				fmt.Println("(nil)")
				fmt.Fprintf(os.Stderr, "%s> ", server)
				continue
			}

			// read l1 bytes
			b := make([]byte, l1)
			if _, err := io.ReadFull(reader, b); err != nil {
				fmt.Println(err)
				break
			}
			fmt.Printf("\"%s\"\n", string(b))
			// discard \r\n
			reader.ReadString('\n')
		case '*': // array
			l := s[1 : len(s)-2]
			l1, err := strconv.Atoi(l)
			if err != nil {
				fmt.Println(err)
				break
			}
			if l1 == -1 || l1 == 0 {
				fmt.Println("(nil)")
			} else {
				narr = l1
				nindex = 0
			}
		default:
			fmt.Printf("unknown response type %v\n", s[0])
		}

		if narr != 0 {
			nindex++
		}
		if nindex > narr {
			narr = 0
		}
		if narr == 0 {
			fmt.Fprintf(os.Stderr, "%s> ", server)
		}
	}
}

var server string

func main() {
	flag.StringVar(&server, "h", "127.0.0.1:6379", "redis server address")
	flag.Parse()

	conn, err := net.Dial("tcp", server)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer conn.Close()

	go readResp(conn)

	r := bufio.NewReader(os.Stdin)
	fmt.Fprintf(os.Stderr, "%s> ", server)
	for {
		s, err := r.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		cmd := encodeCmd(strings.Fields(strings.Trim(s, " \r\n"))...)
		if _, err := io.WriteString(conn, cmd); err != nil {
			fmt.Println(err)
			break
		}
	}
}
