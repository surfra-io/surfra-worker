// Package split breaks a container command into argv without invoking a shell.
package split

import "fmt"

// Command splits a command line the way a POSIX shell splits words.
// Quotes and backslash escapes are honored. There is no expansion, globbing, or substitution.
func Command(line string) ([]string, error) {
	var args []string
	var buf []byte
	quoting := byte(0)
	escaped := false
	started := false
	flush := func() {
		args = append(args, string(buf))
		buf = buf[:0]
		started = false
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		if escaped {
			buf = append(buf, c)
			escaped = false
			started = true
			continue
		}
		if quoting == 0 && c == '\\' {
			escaped = true
			started = true
			continue
		}
		if quoting != 0 {
			if c == quoting {
				quoting = 0
				started = true
				continue
			}
			buf = append(buf, c)
			started = true
			continue
		}
		switch c {
		case ' ', '\t', '\n', '\r':
			if started {
				flush()
			}
		case '\'', '"':
			quoting = c
			started = true
		default:
			buf = append(buf, c)
			started = true
		}
	}
	if escaped {
		return nil, fmt.Errorf("trailing backslash in entrypoint")
	}
	if quoting != 0 {
		return nil, fmt.Errorf("unterminated quote in entrypoint")
	}
	if started {
		flush()
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("entrypoint is empty")
	}
	return args, nil
}
