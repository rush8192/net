package cluster

const (
	GET = iota
	PUT
	UPDATE
	DELETE
)

type CommandType int

type Command struct {
	ctype CommandType
	key string
	value string
}