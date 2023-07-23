package tokenizer

import "bytes"

var keywordMap = map[string]bool{
	"select": true,
	"from":   true,
	"where":  true,
	"insert": true,
	"into":   true,
	"values": true,
	"update": true,
	"set":    true,
	"delete": true,
}

func isBlank(b byte) bool {
	return b == ' ' || b == '\n' || b == '\f'
}

func isAlphaBeta(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isDigital(b byte) bool {
	return b >= '0' && b <= '9'
}

func isIndentifier(b byte) bool {
	return isAlphaBeta(b) || isDigital(b) || b == '_'
}

func isKeyword(b []byte) bool {
	_, ok := keywordMap[string(bytes.ToLower(b))]
	return ok
}
