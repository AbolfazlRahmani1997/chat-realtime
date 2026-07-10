package ports

type InMemoryRepository interface {
	Insert(Key string, Value interface{}) (interface{}, error)
	Get(Key string) (interface{}, error)
}
