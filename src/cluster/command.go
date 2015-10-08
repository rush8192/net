package cluster

const (
	NOOP = iota
	PUT
	UPDATE
	DELETE
	
	// these command types don't get stored in log
	GET
	COMMIT
	FAILED
)

type CommandType int

type Command struct {
	CType CommandType
	Key string
	Value []byte
	CId int64
}