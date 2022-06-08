package storage

import (
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type AmazonTestSuite struct {
	suite.Suite
	BrokenAWSStorage   *AWSStorage
	NoPrefixAWSStorage *AWSStorage
	SSEAWSStorage      *AWSStorage
}

func (suite *AmazonTestSuite) SetupSuite() {
	backend, _ := NewAWSStorage("fake-bucket-dont-exist-klmo123", "", "eu-central-1", "", "")
	suite.BrokenAWSStorage = backend

	s3Bucket := os.Getenv("TEST_STORAGE_AWS_BUCKET")
	s3Region := os.Getenv("TEST_STORAGE_AWS_REGION")
	backend, _ = NewAWSStorage(s3Bucket, "", s3Region, "", "")
	suite.NoPrefixAWSStorage = backend

	backend, _ = NewAWSStorage(s3Bucket, "ssetest", s3Region, "", "AES256")
	suite.SSEAWSStorage = backend

	data := []byte("some object")
	path := "deleteme.txt"

	err := suite.NoPrefixAWSStorage.PutObject(path, data)
	suite.Nil(err, "no error putting deleteme.txt using AmazonS3 backend")

	err = suite.SSEAWSStorage.PutObject(path, data)
	suite.Nil(err, "no error putting deleteme.txt using AmazonS3 backend (SSE)")
}

func (suite *AmazonTestSuite) TearDownSuite() {
	err := suite.NoPrefixAWSStorage.DeleteObject("deleteme.txt")
	suite.Nil(err, "no error deleting deleteme.txt using AmazonS3 backend")

	err = suite.SSEAWSStorage.DeleteObject("deleteme.txt")
	suite.Nil(err, "no error deleting deleteme.txt using AmazonS3 backend")
}

func (suite *AmazonTestSuite) TestListObjects() {
	_, err := suite.BrokenAWSStorage.ListObjects("")
	suite.NotNil(err, "cannot list objects with bad bucket")

	_, err = suite.NoPrefixAWSStorage.ListObjects("")
	suite.Nil(err, "can list objects with good bucket, no prefix")

	_, err = suite.SSEAWSStorage.ListObjects("")
	suite.Nil(err, "can list objects with good bucket, SSE")
}

func (suite *AmazonTestSuite) TestGetObject() {
	_, err := suite.BrokenAWSStorage.GetObject("this-file-cannot-possibly-exist.tgz")
	suite.NotNil(err, "cannot get objects with bad bucket")

	obj, err := suite.SSEAWSStorage.GetObject("deleteme.txt")
	suite.Equal([]byte("some object"), obj.Data, "able to get object with SSE")
}

func (suite *AmazonTestSuite) TestPutObject() {
	err := suite.BrokenAWSStorage.PutObject("this-file-will-not-upload.txt", []byte{})
	suite.NotNil(err, "cannot put objects with bad bucket")
}

func TestAmazonStorageTestSuite(t *testing.T) {
	if os.Getenv("TEST_CLOUD_STORAGE") == "1" &&
		os.Getenv("TEST_STORAGE_AMAZON_BUCKET") != "" &&
		os.Getenv("TEST_STORAGE_AMAZON_REGION") != "" {
		suite.Run(t, new(AmazonTestSuite))
	}
}
