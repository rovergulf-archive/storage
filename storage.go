package storage

// Backend is a generic interface for storage backends
type Backend interface {
	ListObjects(prefix string) ([]Object, error)
	GetObject(key string) (Object, error)
	PutObject(key string, data []byte) error
	DeleteObject(key string) error
	//SyncObjects(src string, dst string) error
}
