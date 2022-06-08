package storage

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	"time"
)

type StorageTestSuite struct {
	suite.Suite
	StorageBackends map[string]Backend
	TempDirectory   string
}

func (suite *StorageTestSuite) setupStorageBackends() {
	timestamp := time.Now().Format("20060102150405")
	suite.TempDirectory = fmt.Sprintf("../../.test/storage-storage/%s", timestamp)
	suite.StorageBackends = make(map[string]Backend)
	ls, err := NewDirStorage(suite.TempDirectory)
	if err != nil {
		suite.Error(err)
	}
	suite.StorageBackends["LocalFilesystem"] = Backend(ls)

	// create empty dir in local storage to make sure it doesnt end up in ListObjects
	if err := os.MkdirAll(fmt.Sprintf("%s/%s", suite.TempDirectory, "ignoreme"), 0777); err != nil {
		suite.Nil(err, "No error creating ignored dir in local storage")
	}

	if os.Getenv("TEST_CLOUD_STORAGE") == "1" {
		prefix := fmt.Sprintf("unittest/%s", timestamp)
		s3Bucket := os.Getenv("TEST_STORAGE_AWS_BUCKET")
		s3Region := os.Getenv("TEST_STORAGE_AWS_REGION")
		gcsBucket := os.Getenv("TEST_STORAGE_GOOGLE_BUCKET")
		if s3Bucket != "" && s3Region != "" {
			s3, err := NewAWSStorage(s3Bucket, prefix, s3Region, "", "")
			if err != nil {
				suite.Error(err)
			}
			suite.StorageBackends["AmazonS3"] = Backend(s3)
		}
		if gcsBucket != "" {
			gcs, err := NewGCPStorage(gcsBucket, prefix)
			if err != nil {
				suite.Error(err)
			}
			suite.StorageBackends["GoogleCS"] = Backend(gcs)
		}
	}
}

func (suite *StorageTestSuite) SetupSuite() {
	suite.setupStorageBackends()

	for i := 1; i <= 9; i++ {
		data := []byte(fmt.Sprintf("test content %d", i))
		path := fmt.Sprintf("test%d.txt", i)
		for key, backend := range suite.StorageBackends {
			err := backend.PutObject(path, data)
			message := fmt.Sprintf("no error putting object %s using %s backend", path, key)
			suite.Nil(err, message)
		}
	}

	for key, backend := range suite.StorageBackends {
		if key == "LocalFilesystem" {
			continue
		}
		data := []byte("skipped object")
		path := "this/is/a/skipped/object.txt"
		err := backend.PutObject(path, data)
		message := fmt.Sprintf("no error putting skipped object %s using %s backend", path, key)
		suite.Nil(err, message)
	}
}

func (suite *StorageTestSuite) TearDownSuite() {
	defer os.RemoveAll(suite.TempDirectory)

	for i := 1; i <= 9; i++ {
		path := fmt.Sprintf("test%d.txt", i)
		for key, backend := range suite.StorageBackends {
			err := backend.DeleteObject(path)
			message := fmt.Sprintf("no error deleting object %s using %s backend", path, key)
			suite.Nil(err, message)
		}
	}

	for key, backend := range suite.StorageBackends {
		if key == "LocalFilesystem" {
			continue
		}
		path := "this/is/a/skipped/object.txt"
		err := backend.DeleteObject(path)
		message := fmt.Sprintf("no error deleting skipped object %s using %s backend", path, key)
		suite.Nil(err, message)
	}
}

func (suite *StorageTestSuite) TestListObjects() {
	for key, backend := range suite.StorageBackends {
		objects, err := backend.ListObjects("")
		message := fmt.Sprintf("no error listing objects using %s backend", key)
		suite.Nil(err, message)
		expectedNumObjects := 9
		message = fmt.Sprintf("%d objects listed using %s backend", expectedNumObjects, key)
		suite.Equal(expectedNumObjects, len(objects), message)
		for i, object := range objects {
			path := fmt.Sprintf("test%d.txt", (i + 1))
			message = fmt.Sprintf("object %s found in list objects using %s backend", path, key)
			suite.Equal(path, object.Path, message)
		}
	}
}

func (suite *StorageTestSuite) TestGetObject() {
	for key, backend := range suite.StorageBackends {
		for i := 1; i <= 9; i++ {
			path := fmt.Sprintf("test%d.txt", i)
			object, err := backend.GetObject(path)
			message := fmt.Sprintf("no error getting object %s using %s backend", path, key)
			suite.Nil(err, message)
			message = fmt.Sprintf("object %s content as expected using %s backend", path, key)
			suite.Equal(object.Data, []byte(fmt.Sprintf("test content %d", i)), message)
		}
	}
}

func (suite *StorageTestSuite) TestHasSuffix() {
	now := time.Now()
	o1 := Object{
		Path:         "mychart-0.1.0.tgz",
		Data:         []byte{},
		LastModified: now,
	}
	suite.True(o1.HasExtension("tgz"), "object has tgz suffix")
	o2 := Object{
		Path:         "mychart-0.1.0.txt",
		Data:         []byte{},
		LastModified: now,
	}
	suite.False(o2.HasExtension("tgz"), "object does not have tgz suffix")
}

func (suite *StorageTestSuite) TestGetObjectSliceDiff() {
	now := time.Now()
	os1 := []Object{
		{
			Path:         "test1.txt",
			Data:         []byte{},
			LastModified: now,
		},
	}
	var os2 []Object
	diff := GetObjectSliceDiff(os1, os2, time.Duration(0))
	suite.True(diff.Change, "change detected")
	suite.Equal(diff.Removed, os1, "removed slice populated")
	suite.Empty(diff.Added, "added slice empty")
	suite.Empty(diff.Updated, "updated slice empty")

	os2 = append(os2, os1[0])
	diff = GetObjectSliceDiff(os1, os2, time.Duration(0))
	suite.False(diff.Change, "no change detected")
	suite.Empty(diff.Removed, "removed slice empty")
	suite.Empty(diff.Added, "added slice empty")
	suite.Empty(diff.Updated, "updated slice empty")

	os2[0].LastModified = now.Add(1)
	diff = GetObjectSliceDiff(os1, os2, time.Duration(0))
	suite.True(diff.Change, "change detected")
	suite.Empty(diff.Removed, "removed slice empty")
	suite.Empty(diff.Added, "added slice empty")
	suite.Equal(diff.Updated, os2, "updated slice populated")

	os2[0].LastModified = now.Add(time.Second)
	diff = GetObjectSliceDiff(os1, os2, time.Second)
	suite.False(diff.Change, "no change detected")
	suite.Empty(diff.Removed, "removed slice empty")
	suite.Empty(diff.Added, "added slice empty")
	suite.Empty(diff.Updated, "updated slice empty")

	os2[0].LastModified = now.Add(time.Second + time.Nanosecond)
	diff = GetObjectSliceDiff(os1, os2, time.Second)
	suite.True(diff.Change, "change detected")
	suite.Empty(diff.Removed, "removed slice empty")
	suite.Empty(diff.Added, "added slice empty")
	suite.Equal(diff.Updated, os2, "updated slice populated")

	os2[0].LastModified = now
	os2 = append(os2, Object{
		Path:         "test2.txt",
		Data:         []byte{},
		LastModified: now,
	})
	diff = GetObjectSliceDiff(os1, os2, time.Duration(0))
	suite.True(diff.Change, "change detected")
	suite.Empty(diff.Removed, "removed slice empty")
	suite.Equal(diff.Added, []Object{os2[1]}, "added slice empty")
	suite.Empty(diff.Updated, "updated slice empty")

}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}
