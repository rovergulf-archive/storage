package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type GoogleTestSuite struct {
	suite.Suite
	BrokenGoogleCSBackend   *GCPStorage
	NoPrefixGoogleCSBackend *GCPStorage
}

func (suite *GoogleTestSuite) SetupSuite() {
	backend, err := NewGCPStorage("fake-bucket-cant-exist-fbce123", "")
	if err != nil {
		suite.Error(err)
	}
	suite.BrokenGoogleCSBackend = backend

	gcsBucket := os.Getenv("TEST_STORAGE_GOOGLE_BUCKET")
	backend, err = NewGCPStorage(gcsBucket, "")
	if err != nil {
		suite.Error(err)
	}
	suite.NoPrefixGoogleCSBackend = backend

	data := []byte("some object")
	path := "deleteme.txt"
	suite.Nil(
		suite.NoPrefixGoogleCSBackend.PutObject(path, data),
		"no error putting deleteme.txt using GoogleCS backend",
	)
}

func (suite *GoogleTestSuite) TearDownSuite() {
	err := suite.NoPrefixGoogleCSBackend.DeleteObject("deleteme.txt")
	suite.Nil(err, "no error deleting deleteme.txt using GoogleCS backend")
}

func (suite *GoogleTestSuite) TestListObjects() {
	_, err := suite.BrokenGoogleCSBackend.ListObjects("")
	suite.NotNil(err, "cannot list objects with bad bucket")

	_, err = suite.NoPrefixGoogleCSBackend.ListObjects("")
	suite.Nil(err, "can list objects with good bucket, no prefix")
}

func (suite *GoogleTestSuite) TestGetObject() {
	_, err := suite.BrokenGoogleCSBackend.GetObject("this-file-cannot-possibly-exist.tgz")
	suite.NotNil(err, "cannot get objects with bad bucket")
}

func (suite *GoogleTestSuite) TestPutObject() {
	err := suite.BrokenGoogleCSBackend.PutObject("this-file-will-not-upload.txt", []byte{})
	suite.NotNil(err, "cannot put objects with bad bucket")
}

func TestGoogleStorageTestSuite(t *testing.T) {
	if os.Getenv("TEST_CLOUD_STORAGE") == "1" &&
		os.Getenv("TEST_STORAGE_GOOGLE_BUCKET") != "" {
		suite.Run(t, new(GoogleTestSuite))
	}
}
