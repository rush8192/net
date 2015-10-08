package cluster

const (
	NOOP = iota
	GET
	PUT
	UPDATE
	DELETE
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