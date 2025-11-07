package parse

import (
	"errors"
	"fmt"
	"strings"
	"text/scanner"
)

var ErrBadSyntax = errors.New("bad syntax")

type Lexer struct {
	keywords map[string]bool
	scanner  scanner.Scanner
	token    rune
	tokenVal string
}

func NewLexer(input string) *Lexer {
	keywords := map[string]bool{
		"select": true, "from": true, "where": true, "and": true,
		"insert": true, "into": true, "values": true,
		"delete": true, "update": true, "set": true,
		"create": true, "table": true, "varchar": true, "int": true,
		"view": true, "as": true, "index": true, "on": true,
	}

	l := &Lexer{
		keywords: keywords,
	}

	l.scanner.Init(strings.NewReader(input))
	l.scanner.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings | scanner.ScanComments | scanner.SkipComments
	l.scanner.Whitespace = 1<<'\t' | 1<<'\n' | 1<<'\r' | 1<<' '

	l.nextToken()
	return l
}

// nextToken advances to the next token and updates token/tokenVal.
// The scanner doesn't handle single-quoted strings, so we parse them manually.
func (l *Lexer) nextToken() {
	l.token = l.scanner.Scan()
	l.tokenVal = l.scanner.TokenText()

	if l.token == '\'' {
		var sb strings.Builder
		for {
			ch := l.scanner.Next()
			if ch == scanner.EOF {
				break
			}
			if ch == '\'' {
				// Two consecutive quotes means an escaped quote
				if l.scanner.Peek() == '\'' {
					sb.WriteRune('\'')
					l.scanner.Next() // consume the second quote
				} else {
					// Single quote means end of string
					break
				}
			} else {
				sb.WriteRune(ch)
			}
		}
		l.tokenVal = sb.String() // Store unquoted string value
		// Keep token as '\'' to mark it as a string constant
		return
	}

	if l.token == scanner.Ident {
		l.tokenVal = strings.ToLower(l.tokenVal)
	}
}

// MatchDelim checks if the current token is the specified delimiter.
func (l *Lexer) MatchDelim(d rune) bool {
	return l.token == d
}

// MatchIntConstant checks if the current token is an integer constant.
func (l *Lexer) MatchIntConstant() bool {
	return l.token == scanner.Int
}

// MatchStringConstant checks if the current token is a string constant (single or double quoted).
func (l *Lexer) MatchStringConstant() bool {
	return l.token == '\'' || l.token == scanner.String
}

// MatchKeyword checks if the current token is the specified keyword (case-insensitive).
func (l *Lexer) MatchKeyword(w string) bool {
	return l.token == scanner.Ident && strings.EqualFold(l.tokenVal, w)
}

// MatchId checks if the current token is an identifier (not a keyword).
func (l *Lexer) MatchId() bool {
	return l.token == scanner.Ident && !l.keywords[strings.ToLower(l.tokenVal)]
}

// EatDelim consumes the current token if it matches the specified delimiter, then advances to the next token.
// Returns ErrBadSyntax if the token doesn't match.
func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return ErrBadSyntax
	}
	l.nextToken()
	return nil
}

// EatIntConstant consumes the current token if it's an integer constant, then advances to the next token.
// Returns the integer value and ErrBadSyntax if the token is not an integer.
func (l *Lexer) EatIntConstant() (int, error) {
	if !l.MatchIntConstant() {
		return 0, ErrBadSyntax
	}

	var i int
	_, err := fmt.Sscanf(l.tokenVal, "%d", &i)
	if err != nil {
		return 0, ErrBadSyntax
	}

	l.nextToken()
	return i, nil
}

// EatStringConstant consumes the current token if it's a string constant, then advances to the next token.
// Returns the unquoted string value and ErrBadSyntax if the token is not a string.
func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", ErrBadSyntax
	}

	s := l.tokenVal
	if l.token == scanner.String && len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	l.nextToken()
	return s, nil
}

// EatKeyword consumes the current token if it matches the specified keyword (case-insensitive), then advances to the next token.
// Returns ErrBadSyntax if the token is not the expected keyword.
func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return ErrBadSyntax
	}
	l.nextToken()
	return nil
}

// EatId consumes the current token if it's an identifier (not a keyword), then advances to the next token.
// Returns the identifier name and ErrBadSyntax if the token is not an identifier.
func (l *Lexer) EatId() (string, error) {
	if !l.MatchId() {
		return "", ErrBadSyntax
	}
	s := l.tokenVal
	l.nextToken()
	return s, nil
}
