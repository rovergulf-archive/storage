package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
	"io/ioutil"
	"time"
)

type etcdStorage struct {
	logger *zap.SugaredLogger
	Client *clientv3.Client
	ctx    context.Context
}

type EtcdOptions struct {
	Logger *zap.SugaredLogger
	Config *clientv3.Config
}

func NewEtcdStorage(opts EtcdOptions) (*etcdStorage, error) {
	ctx := context.Background()
	endpoints := viper.GetStringSlice("etcd.endpoints")

	etcdConf := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	if viper.GetBool("etcd.ssl.enabled") {
		opts.Logger.Debug("Connecting with etcd-client TLS credentials")
		etcdConf.TLS = &tls.Config{
			InsecureSkipVerify: viper.GetBool("etcd.ssl.verify"),
		}

		caFile := viper.GetString("etcd.ssl.ca")
		certFile := viper.GetString("etcd.ssl.cert")
		keyFile := viper.GetString("etcd.ssl.key")

		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, err
			}
			etcdConf.TLS.Certificates = []tls.Certificate{cert}
		}

		if caFile != "" {
			caCert, err := ioutil.ReadFile(caFile)
			if err != nil {
				return nil, err
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			etcdConf.TLS.RootCAs = caCertPool
		}
	}

	cli, err := clientv3.New(etcdConf)
	if err != nil {
		//opts.Logger.Errorf("Unable to start etcd client: %s", err)
		return nil, err
	}

	statusContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.Status(statusContext, endpoints[0]); err != nil {
		opts.Logger.Errorf("Unable to check etcd client status: %s", err)
		return nil, err
	}
	//s.logger.Debug("etcd client status is ok")

	// set namespace prefix
	etcdNamespace := viper.GetString("etcd.namespace")
	if len(etcdNamespace) == 0 {
		etcdNamespace = fmt.Sprintf("/%s/%s/", viper.GetString("app.name"), viper.GetString("app.env"))
		viper.Set("etcd.namespace", etcdNamespace)
		viper.SafeWriteConfig()
	}

	cli.KV = namespace.NewKV(cli.KV, etcdNamespace)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, etcdNamespace)
	cli.Lease = namespace.NewLease(cli.Lease, etcdNamespace)

	return &etcdStorage{
		logger: opts.Logger,
		Client: cli,
		ctx:    ctx,
	}, nil
}

func (s *etcdStorage) GetObject(key string) (Object, error) {
	//
	res, err := s.Client.Get(s.ctx, key)
	if err != nil {
		return Object{}, err
	}

	if len(res.Kvs) == 0 {
		return Object{}, fmt.Errorf("no results")
	}

	val := res.Kvs[0]
	return Object{
		Meta: Metadata{
			Name:    "",
			Version: "",
		},
		Path: key,
		Data: val.Key,
		//LastModified: time.Time{},
	}, nil
}

func (s *etcdStorage) PutObject(key string, data []byte) error {
	//
	_, err := s.Client.Put(s.ctx, key, string(data))
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdStorage) DeleteObject(key string) error {
	//
	_, err := s.Client.Delete(s.ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdStorage) ListObjects(prefix string) ([]Object, error) {
	//
	res, err := s.Client.Get(s.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var result []Object
	for i := range res.Kvs {
		val := res.Kvs[i]
		result = append(result, Object{
			Meta: Metadata{
				Name:    "",
				Version: "",
			},
			Path: string(val.Key),
			Data: val.Value,
		})
	}

	return result, nil
}
