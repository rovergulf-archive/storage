package storage

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"path"
	"strings"
)

type AWSStorage struct {
	Bucket     string
	Client     *s3.S3
	Downloader *s3manager.Downloader
	Prefix     string
	Uploader   *s3manager.Uploader
	SSE        string
}

func NewAWSStorage(bucket string, prefix string, region string, endpoint string, sse string) (*AWSStorage, error) {
	conf := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(strings.HasPrefix(endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(endpoint != ""),
	}

	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	service := s3.New(sess, conf)

	return &AWSStorage{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}, nil
}

func (s *AWSStorage) ListObjects(prefix string) ([]Object, error) {
	var objects []Object

	prefix = path.Join(s.Prefix, prefix)
	s3Input := &s3.ListObjectsInput{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(prefix),
	}
	for {
		s3Result, err := s.Client.ListObjects(s3Input)
		if err != nil {
			return objects, err
		}
		for _, obj := range s3Result.Contents {
			path := removePrefixFromObjectPath(prefix, *obj.Key)
			if objectPathIsInvalid(path) {
				continue
			}
			object := Object{
				Path:         path,
				Data:         []byte{},
				LastModified: *obj.LastModified,
			}
			objects = append(objects, object)
		}
		if !*s3Result.IsTruncated {
			break
		}
		s3Input.Marker = s3Result.Contents[len(s3Result.Contents)-1].Key
	}
	return objects, nil
}

func (s *AWSStorage) GetObject(key string) (Object, error) {
	var object Object
	object.Path = key
	var content []byte
	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path.Join(s.Prefix, key)),
	}
	s3Result, err := s.Client.GetObject(s3Input)
	if err != nil {
		return object, err
	}
	content, err = ioutil.ReadAll(s3Result.Body)
	if err != nil {
		return object, err
	}
	object.Data = content
	object.LastModified = *s3Result.LastModified
	return object, nil
}

func (s *AWSStorage) PutObject(key string, data []byte) error {
	s3Input := &s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path.Join(s.Prefix, key)),
		Body:   bytes.NewBuffer(data),
	}

	if s.SSE != "" {
		s3Input.ServerSideEncryption = aws.String(s.SSE)
	}

	_, err := s.Uploader.Upload(s3Input)
	return err
}

func (s *AWSStorage) DeleteObject(key string) error {
	s3Input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path.Join(s.Prefix, key)),
	}
	_, err := s.Client.DeleteObject(s3Input)
	return err
}
