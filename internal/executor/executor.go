package executor

type Executable interface {
	execute() error
}
