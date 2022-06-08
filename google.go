package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/iterator"
	"io/ioutil"
	"path"
)

type GCPStorage struct {
	prefix string
	bucket string
	client *storage.BucketHandle
	ctx    context.Context
}

func NewGCPStorage(bucket string, prefix string) (*GCPStorage, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucketHandle := client.Bucket(bucket)
	prefix = cleanPrefix(prefix)

	return &GCPStorage{
		ctx:    ctx,
		bucket: bucket,
		prefix: prefix,
		client: bucketHandle,
	}, nil
}

func (s *GCPStorage) GetObject(key string) (Object, error) {
	var object Object
	object.Path = key
	objectHandle := s.client.Object(path.Join(s.prefix, key))
	attrs, err := objectHandle.Attrs(s.ctx)
	if err != nil {
		return object, err
	}
	object.LastModified = attrs.Updated
	rc, err := objectHandle.NewReader(s.ctx)
	if err != nil {
		return object, err
	}
	content, err := ioutil.ReadAll(rc)
	rc.Close()
	if err != nil {
		return object, err
	}
	object.Data = content
	return object, nil
}

// PutObject uploads an object to Google Cloud Storage bucket, at prefix
func (s *GCPStorage) PutObject(key string, content []byte) error {
	wc := s.client.Object(path.Join(s.prefix, key)).NewWriter(s.ctx)
	_, err := wc.Write(content)
	if err != nil {
		return err
	}
	err = wc.Close()
	return err
}

// DeleteObject removes an object from Google Cloud Storage bucket, at prefix
func (s *GCPStorage) DeleteObject(key string) error {
	err := s.client.Object(path.Join(s.prefix, key)).Delete(s.ctx)
	return err
}

func (s *GCPStorage) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	prefix = path.Join(s.prefix, prefix)
	listQuery := &storage.Query{
		Prefix: prefix,
	}
	it := s.client.Objects(s.ctx, listQuery)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return objects, err
		}
		key := removePrefixFromObjectPath(prefix, attrs.Name)
		if objectPathIsInvalid(key) {
			continue
		}
		object := Object{
			Path:         key,
			Data:         []byte{},
			LastModified: attrs.Updated,
		}
		objects = append(objects, object)
	}
	return objects, nil
}
