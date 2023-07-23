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
	tokenMetaCommand tokenType = iota // identifier start with '.'
	tokenKeyword
	tokenIndentifier
	tokenDigit
	tokenString
	tokenEq    // =
	tokenLP    // (
	tokenRP    // )
	tokenComma // ,
	tokenStar  // *
	tokenNull  // special token when a error occured or no more str to tokenize
)

func (t tokenType) String() string {
	switch t {
	case tokenMetaCommand:
		return "metaCommand"
	case tokenKeyword:
		return "keyword"
	case tokenIndentifier:
		return "indentifier"
	case tokenDigit:
		return "digit"
	case tokenString:
		return "string"
	case tokenEq:
		return "equal"
	case tokenLP:
		return "leftParentheses"
	case tokenRP:
		return "rightParentheses"
	case tokenComma:
		return "comma"
	case tokenStar:
		return "star"
	case tokenNull:
		return "nullString"
	default:
		return "unexpected token"
	}
}

type token struct {
	tokenType tokenType
	value     string
}

func (t token) String() string {
	return "{" + fmt.Sprint(t.tokenType) + ": " + t.value + "}"
}

type tokenizer struct {
	str []byte
	pos int

	err error
}

func NewTokenizer(str string) tokenizer {
	return tokenizer{str: []byte(str), pos: 0, err: nil}
}

// return true if eof, otherwise false
func (tk *tokenizer) peekByte() (byte, bool) {
	if tk.pos == len(tk.str) {
		return 0, true
	}
	return tk.str[tk.pos], false
}

func (tk *tokenizer) popByte() {
	if tk.pos < len(tk.str) {
		tk.pos++
	}
}

func (tk *tokenizer) Next() (token, error) {
	if tk.err != nil {
		return token{tokenNull, ""}, tk.err
	}
	return tk.nextMetaState()
}

func (tk *tokenizer) nextMetaState() (token, error) {
	for {
		b, eof := tk.peekByte()
		if eof {
			return token{tokenNull, ""}, errorEndofFile
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
		return token{tokenEq, "="}, nil
	case '"':
		return tk.nextQuoteState()
	case '(':
		tk.popByte()
		return token{tokenLP, "("}, nil
	case ')':
		tk.popByte()
		return token{tokenRP, ")"}, nil
	case ',':
		tk.popByte()
		return token{tokenComma, ","}, nil
	case '*':
		tk.popByte()
		return token{tokenStar, "*"}, nil
	default:
		if isAlphaBeta(b) || isDigital(b) {
			return tk.nextTokenState()
		}
		tk.err = errorInvaildState
		return token{tokenNull, ""}, tk.err
	}
}

func (tk *tokenizer) nextMetaCommandState() (token, error) {
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof || !(isAlphaBeta(b) || isDigital(b)) {
			if isBlank(b) {
				tk.popByte()
			}
			return token{tokenMetaCommand, string(tmp)}, nil
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
}

func (tk *tokenizer) nextQuoteState() (token, error) {
	quote, _ := tk.peekByte()
	tk.popByte()
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof {
			tk.err = errorInvaildState
			return token{tokenNull, ""}, tk.err
		}
		if b == quote {
			tk.popByte()
			break
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
	return token{tokenString, string(tmp)}, nil
}

func (tk *tokenizer) nextTokenState() (token, error) {
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
				return token{tokenDigit, string(tmp)}, nil
			} else if isKeyword(tmp) {
				return token{tokenKeyword, string(tmp)}, nil
			} else {
				return token{tokenIndentifier, string(tmp)}, nil
			}
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
}
