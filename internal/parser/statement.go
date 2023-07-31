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
	len     int // Only meaningful when the varType is VarTypeVarchar
}

type ColumnValue struct {
	varType VarType
	value   []byte
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

type InsertStatement struct {
	TableName string
	Values    []ColumnValue
}
