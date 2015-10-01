package cluster

const (
	GET = iota
	PUT
	UPDATE
	DELETE
)

type CommandType int

type Command struct {
	CType CommandType
	Key string
	Value []byte
}