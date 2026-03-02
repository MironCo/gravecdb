package cypher

import (
	"strings"
	"unicode"
)

// Lexer tokenizes a Cypher query string
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
	line         int
	column       int
}

// NewLexer creates a new Lexer instance
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input, line: 1, column: 0}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII NUL = end of input
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	l.column++
	if l.ch == '\n' {
		l.line++
		l.column = 0
	}
}

// peekChar returns the next character without advancing
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	tok.Line = l.line
	tok.Column = l.column

	switch l.ch {
	case '=':
		tok = l.newToken(TOKEN_EQ, l.ch)
	case '+':
		tok = l.newToken(TOKEN_PLUS, l.ch)
	case '*':
		tok = l.newToken(TOKEN_STAR, l.ch)
	case '/':
		if l.peekChar() == '/' {
			// Line comment - skip to end of line
			l.skipLineComment()
			return l.NextToken()
		}
		tok = l.newToken(TOKEN_SLASH, l.ch)
	case '%':
		tok = l.newToken(TOKEN_PERCENT, l.ch)
	case '^':
		tok = l.newToken(TOKEN_CARET, l.ch)
	case ',':
		tok = l.newToken(TOKEN_COMMA, l.ch)
	case ':':
		tok = l.newToken(TOKEN_COLON, l.ch)
	case ';':
		tok = l.newToken(TOKEN_SEMICOLON, l.ch)
	case '|':
		tok = l.newToken(TOKEN_PIPE, l.ch)
	case '$':
		l.readChar() // consume $
		name := l.readIdentifier()
		tok.Type = TOKEN_PARAM
		tok.Literal = name
		return tok
	case '(':
		tok = l.newToken(TOKEN_LPAREN, l.ch)
	case ')':
		tok = l.newToken(TOKEN_RPAREN, l.ch)
	case '[':
		tok = l.newToken(TOKEN_LBRACKET, l.ch)
	case ']':
		tok = l.newToken(TOKEN_RBRACKET, l.ch)
	case '{':
		tok = l.newToken(TOKEN_LBRACE, l.ch)
	case '}':
		tok = l.newToken(TOKEN_RBRACE, l.ch)
	case '.':
		if l.peekChar() == '.' {
			l.readChar()
			tok.Type = TOKEN_DOTDOT
			tok.Literal = ".."
			l.readChar()
			return tok
		}
		tok = l.newToken(TOKEN_DOT, l.ch)
	case '-':
		if l.peekChar() == '>' {
			l.readChar()
			tok.Type = TOKEN_ARROW_RIGHT
			tok.Literal = "->"
			l.readChar()
			return tok
		}
		tok = l.newToken(TOKEN_DASH, l.ch)
	case '<':
		if l.peekChar() == '-' {
			l.readChar()
			tok.Type = TOKEN_ARROW_LEFT
			tok.Literal = "<-"
			l.readChar()
			return tok
		} else if l.peekChar() == '>' {
			l.readChar()
			tok.Type = TOKEN_NEQ
			tok.Literal = "<>"
			l.readChar()
			return tok
		} else if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_LTE
			tok.Literal = "<="
			l.readChar()
			return tok
		}
		tok = l.newToken(TOKEN_LT, l.ch)
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_GTE
			tok.Literal = ">="
			l.readChar()
			return tok
		}
		tok = l.newToken(TOKEN_GT, l.ch)
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok.Type = TOKEN_NEQ
			tok.Literal = "!="
			l.readChar()
			return tok
		}
		tok = l.newToken(TOKEN_ILLEGAL, l.ch)
	case '"':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString('"')
		return tok
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString('\'')
		return tok
	case 0:
		tok.Literal = ""
		tok.Type = TOKEN_EOF
		return tok
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			// Check for keywords (case-insensitive)
			tok.Type = LookupIdent(strings.ToUpper(tok.Literal))
			return tok
		} else if isDigit(l.ch) {
			tok.Literal, tok.Type = l.readNumber()
			return tok
		} else {
			tok = l.newToken(TOKEN_ILLEGAL, l.ch)
		}
	}

	l.readChar()
	return tok
}

// newToken creates a new token
func (l *Lexer) newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch), Line: l.line, Column: l.column}
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// skipLineComment skips a line comment starting with //
func (l *Lexer) skipLineComment() {
	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}
}

// readIdentifier reads an identifier
func (l *Lexer) readIdentifier() string {
	position := l.position
	// First character must be letter or underscore
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads an integer or float number
func (l *Lexer) readNumber() (string, TokenType) {
	position := l.position
	tokenType := TOKEN_INT

	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		tokenType = TOKEN_FLOAT
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	// Check for exponent
	if l.ch == 'e' || l.ch == 'E' {
		tokenType = TOKEN_FLOAT
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return l.input[position:l.position], tokenType
}

// readString reads a string literal
func (l *Lexer) readString(quote byte) string {
	var sb strings.Builder
	l.readChar() // skip opening quote

	for {
		if l.ch == quote {
			l.readChar() // skip closing quote
			break
		}
		if l.ch == 0 {
			// Unterminated string
			break
		}
		if l.ch == '\\' {
			l.readChar()
			switch l.ch {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			case '\'':
				sb.WriteByte('\'')
			default:
				sb.WriteByte(l.ch)
			}
		} else {
			sb.WriteByte(l.ch)
		}
		l.readChar()
	}

	return sb.String()
}

// isLetter returns true if ch is a letter or underscore
func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

// isDigit returns true if ch is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// PeekToken returns the next token without consuming it
func (l *Lexer) PeekToken() Token {
	// Save current state
	pos := l.position
	readPos := l.readPosition
	ch := l.ch
	line := l.line
	col := l.column

	// Get next token
	tok := l.NextToken()

	// Restore state
	l.position = pos
	l.readPosition = readPos
	l.ch = ch
	l.line = line
	l.column = col

	return tok
}
