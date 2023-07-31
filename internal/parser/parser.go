package parser

import (
	"errors"
	"godb/internal/tokenizer"
	"strconv"
)

var (
	ErrorInvaildStatement = errors.New("invaild statement")
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
	if err != nil {
		return nil, ErrorInvaildStatement
	}
	tk.PopToken()
	switch keyword.Value {
	case "create":
		return parseCreateCommand(tk)
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
		}
		tk.PopToken()
		ct.FieldName = append(ct.FieldName, varName.Value)
		ct.FiledType = append(ct.FiledType, varType)
		if symbol.TokenType == tokenizer.TokenRP {
			break
		}
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
