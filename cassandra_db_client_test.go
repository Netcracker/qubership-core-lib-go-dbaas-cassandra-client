package cassandradbaas

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/gocql/gocql"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	basemodel "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	cassandraConfigLocation   = "/etc/cassandra/cassandra.yaml"
	createDatabaseV3          = "/api/v3/dbaas/test_namespace/databases"
	getDatabaseV3             = "/api/v3/dbaas/test_namespace/databases/get-by-classifier/cassandra"
	cassandraPort             = "9042"
	testContainerUser         = "test_user"
	testContainerPassword     = "test_password"
	testContainerKeyspace     = "service_db"
	testConnectionQuery       = "SELECT release_version FROM system.local"
	changePasswordQueryFormat = "ALTER USER %s WITH PASSWORD '%s'"
)

var (
	cassandraNatPort, _ = nat.NewPort("tcp", cassandraPort)
)

type DatabaseClientTestSuite struct {
	suite.Suite
	database            Database
	cassandraConfigFile *os.File
	cassandraContainer  testcontainers.Container
	cassandraAddress    string
	cassandraPort       nat.Port
	controlSession      *gocql.Session
}

func (suite *DatabaseClientTestSuite) SetupSuite() {
	serviceloader.Register(1, &security.DummyToken{})

	StartMockServer()
	os.Setenv(dbaasAgentUrlProperty, GetMockServerUrl())

	yamlParams := configloader.YamlPropertySourceParams{ConfigFilePath: "testdata/application.yaml"}
	configloader.InitWithSourcesArray(configloader.BasePropertySources(yamlParams))
}

func (suite *DatabaseClientTestSuite) TearDownSuite() {
	os.Unsetenv(dbaasAgentUrlProperty)
	StopMockServer()
}

func (suite *DatabaseClientTestSuite) SetupTest() {
	suite.cassandraConfigFile, _ = ioutil.TempFile("", "cassandra.yaml")
	cassandraConfig, _ := os.ReadFile("./testdata/cassandra.yaml")
	suite.cassandraConfigFile.Write(cassandraConfig)
	suite.cassandraConfigFile.Close()
	suite.T().Cleanup(ClearHandlers)
	dbaasPool := dbaasbase.NewDbaaSPool()
	client := NewClient(dbaasPool)
	suite.database = client.ServiceDatabase()
	ctx := context.Background()
	suite.prepareTestContainer(ctx)
	suite.initDatabase()
}

func (suite *DatabaseClientTestSuite) TearDownTest() {
	os.Remove(suite.cassandraConfigFile.Name())
	err := suite.cassandraContainer.Terminate(context.Background())
	if err != nil {
		suite.T().Fatal(err)
	}
}

func TestDatabaseClientSuite(t *testing.T) {
	suite.Run(t, new(DatabaseClientTestSuite))
}

func (suite *DatabaseClientTestSuite) TestCassandraClient_NewClient() {
	ctx := context.Background()
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := suite.cassandraDbaasResponseHandler(staticPasswordProvider(testContainerPassword))
		writer.Write(jsonString)
	})

	cassandraClient, err := suite.database.GetCassandraClient()
	assert.Nil(suite.T(), err)

	session, err := cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), session)

	suite.checkConnectionIsWorking(session, ctx)
}

func (suite *DatabaseClientTestSuite) TestCassandraClient_GetFromCache() {
	ctx := context.Background()
	counter := 0
	AddHandler(Contains(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := suite.cassandraDbaasResponseHandler(staticPasswordProvider(testContainerPassword))
		writer.Write(jsonString)
		counter++
	})

	cassandraClient, err := suite.database.GetCassandraClient()
	assert.Nil(suite.T(), err)

	firstSession, err := cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), firstSession)
	suite.checkConnectionIsWorking(firstSession, ctx)

	secondSession, err := cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), secondSession)
	assert.Equal(suite.T(), 1, counter)
	suite.checkConnectionIsWorking(secondSession, ctx)
}

func (suite *DatabaseClientTestSuite) TestCassandraDbClient_GetCassandraDatabase_WithLogicalProvider() {
	connectionProperties := map[string]interface{}{
		"username":      testContainerUser,
		"password":      testContainerPassword,
		"contactPoints": []interface{}{suite.cassandraAddress},
		"port":          float64(suite.cassandraPort.Int()),
		"keyspace":      testContainerKeyspace,
	}

	logicalProvider := &TestLogicalDbProvider{ConnectionProperties: connectionProperties, providerCalls: 0}
	dbaasPool := dbaasbase.NewDbaaSPool(basemodel.PoolOptions{
		LogicalDbProviders: []basemodel.LogicalDbProvider{
			logicalProvider,
		},
	})
	client := NewClient(dbaasPool)
	database := client.ServiceDatabase()
	cassandraClient, _ := database.GetCassandraClient()
	ctx := context.Background()
	session, err := cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotEqual(suite.T(), 0, logicalProvider.providerCalls)
	suite.checkConnectionIsWorking(session, ctx)
}

func (suite *DatabaseClientTestSuite) TestCassandraDbClient_GetCassandraDatabase_UpdatePassword() {
	ctx := context.Background()

	clusterConfig := gocql.NewCluster()
	clusterConfig.ConnectTimeout = 5 * time.Second
	cassandraClient, err := suite.database.GetCassandraClient(clusterConfig)
	assert.Nil(suite.T(), err)
	password := testContainerPassword
	AddHandler(matches(createDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := suite.cassandraDbaasResponseHandler(func() string {
			return password
		})
		writer.Write(jsonString)
	})
	AddHandler(matches(getDatabaseV3), func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		jsonString := suite.cassandraDbaasResponseHandler(func() string {
			return password
		})
		writer.Write(jsonString)
	})

	session, err := cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), session)
	suite.checkConnectionIsWorking(session, ctx)

	password = "new_password"
	suite.changePassword(password)
	session, err = cassandraClient.GetSession(ctx)
	assert.Nil(suite.T(), err)
	assert.NotNil(suite.T(), session)
	suite.checkConnectionIsWorking(session, ctx)
}

func (suite *DatabaseClientTestSuite) prepareTestContainer(ctx context.Context) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	req := testcontainers.ContainerRequest{
		Image:        "cassandra:4.1.4",
		ExposedPorts: []string{fmt.Sprintf("%d:%s", 49200, cassandraNatPort.Port())},
		WaitingFor:   NewCassandraSessionWaitStrategy(3*time.Minute, time.Second),
		Mounts:       testcontainers.Mounts(testcontainers.BindMount(suite.cassandraConfigFile.Name(), cassandraConfigLocation)),
	}
	var err error
	suite.cassandraContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          false,
	})
	if err != nil {
		suite.T().Fatal(err)
	}
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.cassandraContainer.Start(ctx)
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.cassandraAddress, err = suite.cassandraContainer.Host(ctx)
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.cassandraPort, err = suite.cassandraContainer.MappedPort(ctx, cassandraNatPort)
	if err != nil {
		suite.T().Fatal(err)
	}

	os.Unsetenv("TESTCONTAINERS_RYUK_DISABLED")
}

func (suite *DatabaseClientTestSuite) initDatabase() {
	data, err := os.ReadFile("./testdata/init_db.cql")
	initScript := string(data)
	statements := strings.Split(initScript, ";")

	clusterConfig := gocql.NewCluster(suite.cassandraAddress)
	clusterConfig.Port = suite.cassandraPort.Int()
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	suite.controlSession, err = clusterConfig.CreateSession()
	if err != nil {
		suite.T().Fatal(err)
	}
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			err = suite.controlSession.Query(statement).Exec()
			if err != nil {
				suite.T().Fatal(err)
			}
		}
	}
}

func (suite *DatabaseClientTestSuite) checkConnectionIsWorking(session *gocql.Session, ctx context.Context) {
	var objectName string
	iter := session.Query("select name from testObjects where id='object1'").Iter()
	iter.Scan(&objectName)
	err := iter.Close()
	assert.Nil(suite.T(), err)
	expectedObjectName := "test object 1"
	assert.Equal(suite.T(), expectedObjectName, objectName)
}

func (suite DatabaseClientTestSuite) cassandraDbaasResponseHandler(passwordProvider func() string) []byte {
	connectionProperties := map[string]interface{}{
		"contactPoints": []string{suite.cassandraAddress},
		"port":          suite.cassandraPort.Int(),
		"keyspace":      testContainerKeyspace,
		"password":      passwordProvider(),
		"username":      testContainerUser,
	}
	dbResponse := basemodel.LogicalDb{
		Id:                   "123",
		ConnectionProperties: connectionProperties,
	}
	jsonResponse, _ := json.Marshal(dbResponse)
	return jsonResponse
}

func (suite *DatabaseClientTestSuite) changePassword(newPassword string) {
	err := suite.controlSession.Query(fmt.Sprintf(changePasswordQueryFormat, testContainerUser, newPassword)).Exec()
	if err != nil {
		suite.T().Error(err)
	}
	ctx := context.Background()
	duration := 3 * time.Second
	// Connection is kept alive indefinitely even when password changes and stopping cassandra is the only way to terminate connection
	if err = suite.cassandraContainer.Stop(ctx, &duration); err != nil {
		suite.T().Fatal(err)
	}
	if err = suite.cassandraContainer.Start(ctx); err != nil {
		suite.T().Fatal(err)
	}
	err = waitForCassandraStart(ctx, time.Minute, time.Second, suite.cassandraAddress, suite.cassandraPort.Int())
	if err != nil {
		suite.T().Error(err)
	}
}

func staticPasswordProvider(password string) func() string {
	return func() string {
		return password
	}
}

func matches(submatch string) func(string) bool {
	return func(path string) bool {
		return strings.EqualFold(path, submatch)
	}
}

type cassandraSessionWaitStrategy struct {
	waitDuration  time.Duration
	checkInterval time.Duration
}

func waitForCassandraStart(ctx context.Context, waitDuration, checkInterval time.Duration, host string, port int) (err error) {
	ctx, cancelContext := context.WithTimeout(ctx, waitDuration)
	defer cancelContext()

	clusterConfig := gocql.NewCluster(host)
	clusterConfig.Port = port
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	var session *gocql.Session
	session, err = clusterConfig.CreateSession()
	for err != nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s:%w", ctx.Err(), err)
		case <-time.After(checkInterval):
			session, err = clusterConfig.CreateSession()
		}
	}
	err = session.Query(testConnectionQuery).Exec()
	for err != nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s:%w", ctx.Err(), err)
		case <-time.After(checkInterval):
			err = session.Query(testConnectionQuery).Exec()
		}
	}
	return
}

func (c cassandraSessionWaitStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) (err error) {
	host, err := target.Host(ctx)
	if err != nil {
		return
	}
	port, err := target.MappedPort(ctx, cassandraNatPort)
	if err != nil {
		return
	}
	return waitForCassandraStart(ctx, c.waitDuration, c.checkInterval, host, port.Int())
}

func NewCassandraSessionWaitStrategy(waitDuration time.Duration, checkInterval time.Duration) *cassandraSessionWaitStrategy {
	return &cassandraSessionWaitStrategy{waitDuration, checkInterval}
}

type TestLogicalDbProvider struct {
	ConnectionProperties map[string]interface{}
	providerCalls        int
}

func (p *TestLogicalDbProvider) GetOrCreateDb(dbType string, classifier map[string]interface{}, params rest.BaseDbParams) (*basemodel.LogicalDb, error) {
	p.providerCalls++
	return &basemodel.LogicalDb{
		Id:                   "123",
		ConnectionProperties: p.ConnectionProperties,
	}, nil
}

func (p *TestLogicalDbProvider) GetConnection(dbType string, classifier map[string]interface{}, params rest.BaseDbParams) (map[string]interface{}, error) {
	p.providerCalls++
	return p.ConnectionProperties, nil
}
