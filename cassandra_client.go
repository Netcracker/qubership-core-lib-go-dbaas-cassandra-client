package cassandradbaas

import (
	"context"

	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client/v4/model"
)

var logger logging.Logger

const (
	DbType = "cassandra"
)

func init() {
	logger = logging.GetLogger("cassandra-dbaas")
}

type DbaaSCassandraClient struct {
	cassandraClientCache cache.DbaaSCache
	pool                 *dbaasbase.DbaaSPool
}

func NewClient(pool *dbaasbase.DbaaSPool) *DbaaSCassandraClient {
	localCache := cache.DbaaSCache{
		LogicalDbCache: make(map[cache.Key]interface{}),
	}
	return &DbaaSCassandraClient{localCache, pool}
}

func (d *DbaaSCassandraClient) ServiceDatabase(params ...model.DbParams) Database {
	return &cassandraDatabase{
		dbaasPool:      d.pool,
		params:         serviceDbParams(params),
		cassandraCache: &d.cassandraClientCache,
	}
}

func (d *DbaaSCassandraClient) TenantDatabase(params ...model.DbParams) Database {
	return &cassandraDatabase{
		dbaasPool:      d.pool,
		params:         tenantDbParams(params),
		cassandraCache: &d.cassandraClientCache,
	}
}

func ServiceClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseServiceClassifier(ctx)
}

func TenantClassifier(ctx context.Context) map[string]interface{} {
	return dbaasbase.BaseTenantClassifier(ctx)
}

func serviceDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = ServiceClassifier
	}
	return localParams
}

func tenantDbParams(params []model.DbParams) model.DbParams {
	localParams := model.DbParams{}
	if params != nil {
		localParams = params[0]
	}
	if localParams.Classifier == nil {
		localParams.Classifier = TenantClassifier
	}
	return localParams
}
