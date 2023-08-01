package parser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/internal/tokenizer"
	"strconv"
)

var (
	ErrorInvaildStatement = errors.New("invaild statement")
	ErrorInternal         = errors.New("internal error")
)

func Parse(statement string) (interface{}, error) {
	tk := tokenizer.NewTokenizer(statement)
	token, err := tk.PeekToken()
	if err != nil {
		return nil, err
	}
	switch token.TokenType {
	case (tokenizer.TokenMetaCommand):
		return parseMetaCommand(tk)
	case (tokenizer.TokenKeyword):
		return parseCommand(tk)
	default:
		return nil, ErrorInvaildStatement
	}
}

func parseMetaCommand(tk tokenizer.Tokenizer) (interface{}, error) {
	var mt MetaCommandType
	var tmp []string
	for {
		token, err := tk.PeekToken()
		if err != nil {
			break
		}
		tk.PopToken()
		switch token.TokenType {
		case tokenizer.TokenMetaCommand:
			switch token.Value {
			case "exit":
				mt = MetaCommandExit
			default:
				return nil, ErrorInvaildStatement
			}
		default:
			tmp = append(tmp, token.Value)
		}
	}
	return ExitMetaStatement{mt, tmp}, nil
}

func parseCommand(tk tokenizer.Tokenizer) (interface{}, error) {
	keyword, err := tk.PeekToken()
	if err != nil || keyword.TokenType != tokenizer.TokenKeyword {
		return nil, ErrorInvaildStatement
	}
	tk.PopToken()
	switch keyword.Value {
	case "create":
		return parseCreateCommand(tk)
	case "insert":
		return parseInsertCommand(tk)
	case "select":
		return parseSelectCommand(tk)
	default:
		return nil, ErrorInvaildStatement
	}
}

func parseCreateCommand(tk tokenizer.Tokenizer) (CreateTableStatement, error) {
	table, err := tk.PeekToken()
	var ct CreateTableStatement
	if err != nil || table.Value != "table" {
		return CreateTableStatement{}, ErrorInvaildStatement
	}
	tk.PopToken()
	tableName, err := tk.PeekToken()
	if err != nil || tableName.TokenType != tokenizer.TokenIdentifier {
		return CreateTableStatement{}, ErrorInvaildStatement
	}
	ct.TableName = tableName.Value
	tk.PopToken()
	lp, err := tk.PeekToken()
	if err != nil || lp.TokenType != tokenizer.TokenLP {
		return CreateTableStatement{}, ErrorInvaildStatement
	}
	tk.PopToken()
	for {
		token, err := tk.PeekToken()
		if err != nil {
			return CreateTableStatement{}, ErrorInvaildStatement
		}
		if token.TokenType == tokenizer.TokenRP {
			tk.PopToken()
			break
		}
		varName, err := tk.PeekToken()
		if err != nil || varName.TokenType != tokenizer.TokenIdentifier {
			return CreateTableStatement{}, ErrorInvaildStatement
		}
		tk.PopToken()
		varType, err := parseType(&tk)
		if err != nil {
			return CreateTableStatement{}, ErrorInvaildStatement
		}
		symbol, err := tk.PeekToken()
		if err != nil {
			return CreateTableStatement{}, ErrorInvaildStatement
		} else if symbol.TokenType == tokenizer.TokenRP {
			break
		} else if symbol.TokenType != tokenizer.TokenComma {
			return CreateTableStatement{}, ErrorInvaildStatement
		}
		tk.PopToken()
		ct.FieldName = append(ct.FieldName, varName.Value)
		ct.FiledType = append(ct.FiledType, varType)
	}
	return ct, nil
}

func parseType(tk *tokenizer.Tokenizer) (ColumnType, error) {
	token, err := tk.PeekToken()
	if err != nil || token.TokenType != tokenizer.TokenKeyword {
		return ColumnType{}, ErrorInvaildStatement
	}
	tk.PopToken()
	switch token.Value {
	case "integer":
		return ColumnType{VarTypeInteger, 0}, nil
	case "varchar":
		lp, err := tk.PeekToken()
		if err != nil || lp.TokenType != tokenizer.TokenLP {
			return ColumnType{}, ErrorInvaildStatement
		}
		tk.PopToken()
		varlenStr, err := tk.PeekToken()
		if err != nil || varlenStr.TokenType != tokenizer.TokenDigit {
			return ColumnType{}, ErrorInvaildStatement
		}
		varlen, err := strconv.Atoi(varlenStr.Value)
		if err != nil {
			return ColumnType{}, ErrorInvaildStatement
		}
		tk.PopToken()
		rp, err := tk.PeekToken()
		if err != nil || rp.TokenType != tokenizer.TokenRP {
			return ColumnType{}, nil
		}
		tk.PopToken()
		return ColumnType{VarTypeVarchar, varlen}, nil
	default:
		return ColumnType{}, ErrorInvaildStatement
	}
}

func parseInsertCommand(tk tokenizer.Tokenizer) (InsertStatement, error) {
	var cv InsertStatement
	token, err := tk.PeekToken()
	if err != nil || token.Value != "into" {
		return InsertStatement{}, ErrorInvaildStatement
	}
	tk.PopToken()
	tableName, err := tk.PeekToken()
	if err != nil || tableName.TokenType != tokenizer.TokenIdentifier {
		return InsertStatement{}, ErrorInvaildStatement
	}
	cv.TableName = tableName.Value
	tk.PopToken()
	keywordValue, err := tk.PeekToken()
	if err != nil || keywordValue.Value != "values" {
		return InsertStatement{}, ErrorInvaildStatement
	}
	tk.PopToken()
	lp, err := tk.PeekToken()
	if err != nil || lp.TokenType != tokenizer.TokenLP {
		return InsertStatement{}, ErrorInvaildStatement
	}
	tk.PopToken()
	for {
		token, err := tk.PeekToken()
		if err != nil {
			return InsertStatement{}, ErrorInvaildStatement
		}
		if token.TokenType == tokenizer.TokenRP {
			tk.PopToken()
			break
		}
		switch token.TokenType {
		case tokenizer.TokenDigit:
			value, err := strconv.Atoi(token.Value)
			buf := new(bytes.Buffer)
			if err != nil {
				return InsertStatement{}, ErrorInvaildStatement
			}
			convert_err := binary.Write(buf, binary.LittleEndian, int32(value))
			if convert_err != nil {
				return InsertStatement{}, ErrorInternal
			}
			cv.Values = append(cv.Values, ColumnValue{VarTypeInteger, buf.Bytes()})
		case tokenizer.TokenString:
			strBytes := []byte(token.Value)
			cv.Values = append(cv.Values, ColumnValue{VarTypeVarchar, strBytes})
		default:
			return InsertStatement{}, ErrorInvaildStatement
		}
		tk.PopToken()
		symbol, err := tk.PeekToken()
		if err != nil {
			return InsertStatement{}, ErrorInvaildStatement
		} else if symbol.TokenType == tokenizer.TokenRP {
			break
		} else if symbol.TokenType != tokenizer.TokenComma {
			return InsertStatement{}, ErrorInvaildStatement
		}
		tk.PopToken()
	}
	return cv, nil
}

func parseSelectCommand(tk tokenizer.Tokenizer) (SelectStatement, error) {
	var cv SelectStatement
	for {
		token, err := tk.PeekToken()
		if err != nil {
			return SelectStatement{}, ErrorInvaildStatement
		}
		if token.TokenType == tokenizer.TokenIdentifier {
			cv.TableName = token.Value
			break
		}
		tk.PopToken()
	}
	return cv, nil
}
