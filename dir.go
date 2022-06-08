package storage

import (
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type DirStorage struct {
	logger  *zap.SugaredLogger
	rootDir string
}

func NewDirStorage(rootDir string) (*DirStorage, error) {
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}

	return &DirStorage{
		rootDir: absPath,
	}, nil
}

func (s *DirStorage) GetObject(key string) (Object, error) {
	var object Object
	object.Path = key
	fullPath := path.Join(s.rootDir, key)

	content, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return object, err
	}

	object.Data = content
	info, err := os.Stat(fullPath)
	if err != nil {
		return object, err
	}
	object.LastModified = info.ModTime()
	return object, err
}

func (s *DirStorage) PutObject(key string, data []byte) error {
	fullPath := path.Join(s.rootDir, key)
	folderPath := path.Dir(fullPath)

	if _, err := os.Stat(folderPath); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(folderPath, 0777); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if err := ioutil.WriteFile(fullPath, data, 0644); err != nil {
		return err
	}

	return nil
}

func (s *DirStorage) DeleteObject(key string) error {
	fullPath := path.Join(s.rootDir, key)
	return os.Remove(fullPath)
}

func (s *DirStorage) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	files, err := ioutil.ReadDir(path.Join(s.rootDir, prefix))
	if err != nil {
		if os.IsNotExist(err) { // OK if the directory doesnt exist yet
			err = nil
		}
		return objects, err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		object := Object{Path: f.Name(), Data: []byte{}, LastModified: f.ModTime()}
		objects = append(objects, object)
	}

	return objects, nil
}
