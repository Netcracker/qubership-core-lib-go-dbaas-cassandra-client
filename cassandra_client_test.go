package cassandradbaas

import (
	"context"
	"os"
	"testing"

	. "github.com/netcracker/qubership-core-lib-go/v3/const"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/tenant"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	"github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client/v3/model"
	"github.com/stretchr/testify/assert"
)

func init() {
	ctxmanager.Register([]ctxmanager.ContextProvider{tenant.TenantProvider{}})
	serviceloader.Register(1, &security.DummyToken{})
}

const (
	microserviceName = "test_service"
	namespace        = "test_namespace"
)

func beforeAll() {
	os.Setenv(MicroserviceNameProperty, microserviceName)
	os.Setenv(NamespaceProperty, namespace)
	configloader.Init(configloader.EnvPropertySource())
}

func afterAll() {
	os.Clearenv()
}

func TestMain(m *testing.M) {
	beforeAll()
	exitCode := m.Run()
	afterAll()
	os.Exit(exitCode)
}

func TestNewServiceDbaasClient_WithoutParams(t *testing.T) {
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	serviceDB := commonClient.ServiceDatabase()
	assert.NotNil(t, serviceDB)
	db := serviceDB.(*cassandraDatabase)
	ctx := context.Background()
	assert.Equal(t, ServiceClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewServiceDbaasClient_WithParams(t *testing.T) {
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	params := model.DbParams{
		Classifier:   stubClassifier,
		BaseDbParams: rest.BaseDbParams{},
	}
	serviceDB := commonClient.ServiceDatabase(params)
	assert.NotNil(t, serviceDB)
	db := serviceDB.(*cassandraDatabase)
	ctx := context.Background()
	assert.Equal(t, stubClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewTenantDbaasClient_WithoutParams(t *testing.T) {
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	tenantDb := commonClient.TenantDatabase()
	assert.NotNil(t, tenantDb)
	db := tenantDb.(*cassandraDatabase)
	ctx := createTenantContext()
	assert.Equal(t, TenantClassifier(ctx), db.params.Classifier(ctx))
}

func TestNewTenantDbaasClient_WithParams(t *testing.T) {
	dbaasPool := dbaasbase.NewDbaaSPool()
	commonClient := NewClient(dbaasPool)
	params := model.DbParams{
		Classifier:   stubClassifier,
		BaseDbParams: rest.BaseDbParams{},
	}
	tenantDb := commonClient.TenantDatabase(params)
	assert.NotNil(t, tenantDb)
	db := tenantDb.(*cassandraDatabase)
	ctx := context.Background()
	assert.Equal(t, stubClassifier(ctx), db.params.Classifier(ctx))
}

func TestServiceClassifier(t *testing.T) {
	expected := map[string]interface{}{
		"microserviceName": microserviceName,
		"namespace":        namespace,
		"scope":            "service",
	}
	actual := ServiceClassifier(context.Background())
	assert.Equal(t, expected, actual)
}

func TestTenantClassifier(t *testing.T) {
	expected := map[string]interface{}{
		"microserviceName": microserviceName,
		"namespace":        namespace,
		"tenantId":         "123",
		"scope":            "tenant",
	}

	ctx := createTenantContext()
	actual := TenantClassifier(ctx)
	assert.Equal(t, expected, actual)
}

func stubClassifier(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"scope":            "service",
		"microserviceName": microserviceName,
	}
}

func createTenantContext() context.Context {
	incomingHeaders := map[string]interface{}{tenant.TenantHeader: "123"}
	return ctxmanager.InitContext(context.Background(), incomingHeaders)
}
