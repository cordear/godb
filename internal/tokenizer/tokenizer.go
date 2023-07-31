package tokenizer

import (
	"errors"
	"fmt"
)

type tokenType int

var (
	errorInvaildState = errors.New("invaild state")
	errorEndofFile    = errors.New("eof")
)

const (
	TokenMetaCommand tokenType = iota // identifier start with '.'
	TokenKeyword
	TokenIdentifier
	TokenDigit
	TokenString
	TokenEq    // =
	TokenLP    // (
	TokenRP    // )
	TokenComma // ,
	TokenStar  // *
	TokenNull  // special token when a error occured or no more str to tokenize
)

func (t tokenType) String() string {
	switch t {
	case TokenMetaCommand:
		return "metaCommand"
	case TokenKeyword:
		return "keyword"
	case TokenIdentifier:
		return "identifier"
	case TokenDigit:
		return "digit"
	case TokenString:
		return "string"
	case TokenEq:
		return "equal"
	case TokenLP:
		return "leftParentheses"
	case TokenRP:
		return "rightParentheses"
	case TokenComma:
		return "comma"
	case TokenStar:
		return "star"
	case TokenNull:
		return "nullString"
	default:
		return "unexpected token"
	}
}

type Token struct {
	TokenType tokenType
	Value     string
}

func (t Token) String() string {
	return "{" + fmt.Sprint(t.TokenType) + ": " + t.Value + "}"
}

type Tokenizer struct {
	str []byte
	pos int

	curToken     Token
	isflushToken bool
	err          error
}

func NewTokenizer(str string) Tokenizer {
	return Tokenizer{str: []byte(str), pos: 0, curToken: Token{TokenNull, ""}, isflushToken: true, err: nil}
}

// return true if eof, otherwise false
func (tk *Tokenizer) peekByte() (byte, bool) {
	if tk.pos == len(tk.str) {
		return 0, true
	}
	return tk.str[tk.pos], false
}

func (tk *Tokenizer) popByte() {
	if tk.pos < len(tk.str) {
		tk.pos++
	}
}

func (tk *Tokenizer) PeekToken() (Token, error) {
	if tk.err != nil {
		return Token{TokenNull, ""}, tk.err
	}
	if tk.isflushToken {
		t, err := tk.next()
		if err != nil {
			tk.err = err
			return Token{TokenNull, ""}, tk.err
		}
		tk.curToken = t
		tk.isflushToken = false
		return tk.curToken, nil
	}
	return tk.curToken, nil
}

func (tk *Tokenizer) PopToken() {
	tk.isflushToken = true
}

func (tk *Tokenizer) next() (Token, error) {
	if tk.err != nil {
		return Token{TokenNull, ""}, tk.err
	}
	return tk.nextMetaState()
}

func (tk *Tokenizer) nextMetaState() (Token, error) {
	for {
		b, eof := tk.peekByte()
		if eof {
			return Token{TokenNull, ""}, errorEndofFile
		}
		if !isBlank(b) {
			break
		}
		tk.popByte()
	}
	b, _ := tk.peekByte()
	switch b {
	case '.':
		tk.popByte()
		return tk.nextMetaCommandState()
	case '=':
		tk.popByte()
		return Token{TokenEq, "="}, nil
	case '\'':
		// do not pop token here
		return tk.nextQuoteState()
	case '(':
		tk.popByte()
		return Token{TokenLP, "("}, nil
	case ')':
		tk.popByte()
		return Token{TokenRP, ")"}, nil
	case ',':
		tk.popByte()
		return Token{TokenComma, ","}, nil
	case '*':
		tk.popByte()
		return Token{TokenStar, "*"}, nil
	default:
		if isAlphaBeta(b) || isDigital(b) {
			return tk.nextTokenState()
		}
		tk.err = errorInvaildState
		return Token{TokenNull, ""}, tk.err
	}
}

func (tk *Tokenizer) nextMetaCommandState() (Token, error) {
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof || !(isAlphaBeta(b) || isDigital(b)) {
			if isBlank(b) {
				tk.popByte()
			}
			return Token{TokenMetaCommand, string(tmp)}, nil
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
}

func (tk *Tokenizer) nextQuoteState() (Token, error) {
	quote, _ := tk.peekByte()
	tk.popByte()
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof {
			tk.err = errorInvaildState
			return Token{TokenNull, ""}, tk.err
		}
		if b == quote {
			tk.popByte()
			break
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
	return Token{TokenString, string(tmp)}, nil
}

func (tk *Tokenizer) nextTokenState() (Token, error) {
	var tmp []byte
	is_number := true
	for {
		b, eof := tk.peekByte()
		if isAlphaBeta(b) || b == '_' {
			is_number = false
		}
		if eof || !isIndentifier(b) {
			if isBlank(b) {
				tk.popByte()
			}
			if is_number {
				return Token{TokenDigit, string(tmp)}, nil
			} else if isKeyword(tmp) {
				return Token{TokenKeyword, string(tmp)}, nil
			} else {
				return Token{TokenIdentifier, string(tmp)}, nil
			}
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
}
