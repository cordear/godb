package parser

type MetaCommandType int
type VarType int

const (
	MetaCommandExit MetaCommandType = iota
)
const (
	VarTypeInteger VarType = iota
	VarTypeVarchar
)

func (vty VarType) String() string {
	switch vty {
	case VarTypeInteger:
		return "integer"
	case VarTypeVarchar:
		return "varchar"
	default:
		return "unknow"
	}
}

type ColumnType struct {
	varType VarType
	len     int
}

type ExitMetaStatement struct {
	CommandType MetaCommandType
	Parameters  []string
}

type CreateTableStatement struct {
	TableName string
	FieldName []string
	FiledType []ColumnType
}
