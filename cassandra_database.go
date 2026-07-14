package cassandradbaas

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client/v4/model"
)

type Database interface {
	GetCassandraClient(config ...*gocql.ClusterConfig) (CassandraDbClient, error)
}

type cassandraDatabase struct {
	dbaasPool      *dbaasbase.DbaaSPool
	params         model.DbParams
	cassandraCache *cache.DbaaSCache
}

func (c cassandraDatabase) GetCassandraClient(config ...*gocql.ClusterConfig) (CassandraDbClient, error) {
	var clusterConfig *gocql.ClusterConfig
	if config != nil {
		clusterConfig = config[0]
	} else {
		clusterConfig = gocql.NewCluster()
	}
	return &cassandraDbClient{
		clusterConfig:  clusterConfig,
		dbaasClient:    c.dbaasPool.Client,
		cassandraCache: c.cassandraCache,
		params:         c.params,
	}, nil
}
