package cassandradbaas

import (
	"os"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	dbaasAgentUrlProperty    = "dbaas.agent"
	testToken                = "test-token"
	testTokenExpiresIn       = 300
)

var (
	username = "test_service"
	password = "test_password"
)

type DatabaseTestSuite struct {
	suite.Suite
	database Database
}

func (suite *DatabaseTestSuite) SetupSuite() {
	StartMockServer()
	os.Setenv(dbaasAgentUrlProperty, GetMockServerUrl())

	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.InitWithSourcesArray(configloader.BasePropertySources(yamlParams))
}

func (suite *DatabaseTestSuite) TearDownSuite() {
	os.Unsetenv(dbaasAgentUrlProperty)
	StopMockServer()
}

func (suite *DatabaseTestSuite) SetupTest() {
	suite.T().Cleanup(ClearHandlers)
	dbaasPool := dbaasbase.NewDbaaSPool()
	client := NewClient(dbaasPool)
	suite.database = client.ServiceDatabase()
}

func TestDatabaseSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}

func (suite *DatabaseTestSuite) TestServiceDbaasCassandraClient_GetCassandraClient_WithoutOptions() {
	actualClient, err := suite.database.GetCassandraClient()
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualClient)
}

func (suite *DatabaseTestSuite) TestServiceDbaasCassandraClient_GetCassandraClient_WithOptions() {
	testConfig := gocql.NewCluster()
	testConfig.ConnectTimeout = 1 * time.Second
	actualClient, err := suite.database.GetCassandraClient(testConfig)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), actualClient)
}
