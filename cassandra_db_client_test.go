package cassandradbaas

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	basemodel "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	. "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/testutils"
	"github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type DatabaseClientTestSuite struct {
	suite.Suite
	database            Database
	cassandraConfigFile *os.File
	cassandraContainer  testcontainers.Container
	cassandraAddress    string
	cassandraPort       int
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
		"port":          float64(suite.cassandraPort),
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
		ExposedPorts: []string{cassandraPort + "/tcp"},
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
	mappedPort, err := suite.cassandraContainer.MappedPort(ctx, cassandraPort)
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.cassandraPort = int(mappedPort.Num())

	os.Unsetenv("TESTCONTAINERS_RYUK_DISABLED")
}

func (suite *DatabaseClientTestSuite) initDatabase() {
	data, err := os.ReadFile("./testdata/init_db.cql")
	initScript := string(data)
	statements := strings.Split(initScript, ";")

	clusterConfig := gocql.NewCluster(suite.cassandraAddress)
	clusterConfig.Port = suite.cassandraPort
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
		"port":          suite.cassandraPort,
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
	mappedPort, err := suite.cassandraContainer.MappedPort(ctx, cassandraPort)
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.cassandraPort = int(mappedPort.Num())
	err = waitForCassandraStart(ctx, time.Minute, time.Second, suite.cassandraAddress, suite.cassandraPort)
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
	port, err := target.MappedPort(ctx, cassandraPort)
	if err != nil {
		return
	}
	return waitForCassandraStart(ctx, c.waitDuration, c.checkInterval, host, int(port.Num()))
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

// TestGetSession_RespectsContextCancellation verifies that GetSession returns when
// the caller's context is cancelled, proving that the context is propagated all the
// way through the public API down to the blocking query inside isPasswordValid.
func (suite *DatabaseClientTestSuite) TestGetSession_RespectsContextCancellation() {
	done := make(chan struct{})
	defer close(done)

	session := newCQLMockSession(suite.T(), done)

	staticClassifier := map[string]interface{}{"scope": "service"}
	classifierFn := func(ctx context.Context) map[string]interface{} { return staticClassifier }
	key := cache.NewKey(DbType, classifierFn(context.Background()))
	client := &cassandraDbClient{
		clusterConfig: gocql.NewCluster(),
		cassandraCache: &cache.DbaaSCache{
			LogicalDbCache: map[cache.Key]interface{}{key: session},
		},
		params: model.DbParams{Classifier: classifierFn},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() { _, err := client.GetSession(ctx); resultCh <- err }()
	select {
	case <-resultCh:
	case <-time.After(2 * time.Second):
		suite.T().Fatal("Context timeout is not taken into account by GetSession")
	}
}

// newCQLMockSession starts a minimal in-process CQL mock server, waits for gocql's
// CreateSession to complete, and returns the connected session. The mock goroutines
// exit cleanly when done is closed.
func newCQLMockSession(t *testing.T, done <-chan struct{}, failFirstCheck ...bool) *gocql.Session {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	var checkFailed atomic.Bool
	failFirst := len(failFirstCheck) > 0 && failFirstCheck[0]
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveCQLMock(conn, done, failFirst, &checkFailed)
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	cfg := gocql.NewCluster(addr.IP.String())
	cfg.Port = addr.Port
	cfg.NumConns = 1
	cfg.DisableInitialHostLookup = true
	cfg.ConnectTimeout = 5 * time.Second
	session, err := cfg.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)
	return session
}

// TestWaitForSessionReconnect_RespectsContextCancellation verifies that
// waitForSessionReconnect returns when the caller's context is cancelled,
// proving that WithContext(ctx) is propagated to the query.
func (suite *DatabaseClientTestSuite) TestWaitForSessionReconnect_RespectsContextCancellation() {
	done := make(chan struct{})
	defer close(done)

	session := newCQLMockSession(suite.T(), done)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() { resultCh <- waitForSessionReconnect(ctx, session, 5*time.Second) }()
	select {
	case <-resultCh:
	case <-time.After(2 * time.Second):
		suite.T().Fatal("Context timeout is not taken into account by waitForSessionReconnect")
	}
}

func TestWaitForSessionReconnect_RetriesWithContext(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	session := newCQLMockSession(t, done, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, waitForSessionReconnect(ctx, session, time.Second))
}

// serveCQLMock speaks enough of the CQL native protocol to let gocql complete
// CreateSession, then hangs on EXECUTE of checkConnectionQuery without responding.
// Goroutines exit cleanly when done is closed.
func serveCQLMock(conn net.Conn, done <-chan struct{}, failFirstCheck bool, checkFailed *atomic.Bool) {
	defer conn.Close()
	prepared := make(map[string]string)
	var nextPreparedID uint32 = 1

	for {
		version, stream, opcode, body, err := cqlReadFrame(conn)
		if err != nil {
			return
		}
		respVersion := version&0x7F | 0x80
		switch opcode {
		case 0x05: // OPTIONS -> SUPPORTED
			cqlWriteSupportedFrame(conn, respVersion, stream)
		case 0x01: // STARTUP -> READY
			cqlWriteReadyFrame(conn, respVersion, stream)
		case 0x0B: // REGISTER -> READY
			cqlWriteReadyFrame(conn, respVersion, stream)
		case 0x07: // QUERY
			q := strings.TrimSpace(cqlReadLongString(body))
			if strings.Contains(strings.ToLower(q), "system.local") {
				cqlWriteSystemLocalFrame(conn, respVersion, stream)
			} else {
				cqlWriteEmptyRowsFrame(conn, respVersion, stream)
			}
		case 0x09: // PREPARE -> PREPARED
			q := strings.TrimSpace(cqlReadLongString(body))
			idBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(idBytes, nextPreparedID)
			prepared[string(idBytes)] = q
			nextPreparedID++
			cqlWritePreparedFrame(conn, respVersion, stream, idBytes)
		case 0x0A: // EXECUTE
			if !cqlHandleExecute(conn, respVersion, stream, body, prepared, done, failFirstCheck, checkFailed) {
				return
			}
		}
	}
}

func cqlHandleExecute(conn net.Conn, version byte, stream uint16, body []byte, prepared map[string]string, done <-chan struct{}, failFirstCheck bool, checkFailed *atomic.Bool) bool {
	if !strings.EqualFold(strings.TrimSpace(cqlPreparedQuery(body, prepared)), checkConnectionQuery) {
		cqlWriteEmptyRowsFrame(conn, version, stream)
		return true
	}
	if failFirstCheck {
		if checkFailed.CompareAndSwap(false, true) {
			cqlWriteErrorFrame(conn, version, stream, "temporary connection error")
		} else {
			cqlWriteEmptyRowsFrame(conn, version, stream)
		}
		return true
	}
	<-done
	return false
}

func cqlReadFrame(conn net.Conn) (byte, uint16, byte, []byte, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, 0, 0, nil, err
	}
	body := make([]byte, int(binary.BigEndian.Uint32(header[5:9])))
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, 0, 0, nil, err
	}
	return header[0], binary.BigEndian.Uint16(header[2:4]), header[4], body, nil
}

func cqlPreparedQuery(body []byte, prepared map[string]string) string {
	if len(body) < 2 {
		return ""
	}
	idLen := int(binary.BigEndian.Uint16(body[0:2]))
	if idLen == 0 || len(body) < 2+idLen {
		return ""
	}
	return prepared[string(body[2:2+idLen])]
}
func cqlReadLongString(body []byte) string {
	if len(body) < 4 {
		return ""
	}
	n := int(binary.BigEndian.Uint32(body[0:4]))
	if len(body) < 4+n {
		return ""
	}
	return string(body[4 : 4+n])
}

func cqlWriteSupportedFrame(conn net.Conn, version byte, stream uint16) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, uint16(1))
	cqlShortString(&body, "CQL_VERSION")
	binary.Write(&body, binary.BigEndian, uint16(1))
	cqlShortString(&body, "3.0.0")
	cqlWriteFrame(conn, version, 0x06, stream, body.Bytes())
}

func cqlWriteErrorFrame(conn net.Conn, version byte, stream uint16, message string) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(0))
	cqlShortString(&body, message)
	cqlWriteFrame(conn, version, 0x00, stream, body.Bytes())
}
func cqlWriteReadyFrame(conn net.Conn, version byte, stream uint16) {
	cqlWriteFrame(conn, version, 0x02, stream, nil)
}

func cqlWriteEmptyRowsFrame(conn net.Conn, version byte, stream uint16) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(2)) // kind = Rows
	binary.Write(&body, binary.BigEndian, int32(0)) // flags
	binary.Write(&body, binary.BigEndian, int32(0)) // columns_count
	binary.Write(&body, binary.BigEndian, int32(0)) // rows_count
	cqlWriteFrame(conn, version, 0x08, stream, body.Bytes())
}

func cqlWriteSystemLocalFrame(conn net.Conn, version byte, stream uint16) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(2))      // kind = Rows
	binary.Write(&body, binary.BigEndian, int32(0x0001)) // flags = Global_tables_spec
	binary.Write(&body, binary.BigEndian, int32(5))      // columns_count
	cqlShortString(&body, "system")
	cqlShortString(&body, "local")
	cqlShortString(&body, "host_id")
	binary.Write(&body, binary.BigEndian, uint16(0x000C)) // uuid
	cqlShortString(&body, "data_center")
	binary.Write(&body, binary.BigEndian, uint16(0x000D)) // varchar
	cqlShortString(&body, "rack")
	binary.Write(&body, binary.BigEndian, uint16(0x000D)) // varchar
	cqlShortString(&body, "tokens")
	binary.Write(&body, binary.BigEndian, uint16(0x0020)) // list
	binary.Write(&body, binary.BigEndian, uint16(0x000D)) // list element type: varchar
	cqlShortString(&body, "partitioner")
	binary.Write(&body, binary.BigEndian, uint16(0x000D)) // varchar
	binary.Write(&body, binary.BigEndian, int32(1))       // rows_count = 1
	binary.Write(&body, binary.BigEndian, int32(16))      // host_id: 16-byte UUID
	body.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	cqlWriteBytes(&body, []byte("datacenter1"))
	cqlWriteBytes(&body, []byte("rack1"))
	binary.Write(&body, binary.BigEndian, int32(4)) // tokens: empty list (4-byte length prefix)
	binary.Write(&body, binary.BigEndian, int32(0)) // element count = 0
	cqlWriteBytes(&body, []byte("org.apache.cassandra.dht.Murmur3Partitioner"))
	cqlWriteFrame(conn, version, 0x08, stream, body.Bytes())
}

// cqlWritePreparedFrame returns a minimal PREPARED result so gocql can cache
// the statement and later send EXECUTE frames for it.
func cqlWritePreparedFrame(conn net.Conn, version byte, stream uint16, id []byte) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(4))        // kind = Prepared
	binary.Write(&body, binary.BigEndian, uint16(len(id))) // prepared ID (short bytes)
	body.Write(id)
	// params metadata: flags=0, colCount=0, pkeyCount=0
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	// result metadata: flags=0, colCount=0
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int32(0))
	cqlWriteFrame(conn, version, 0x08, stream, body.Bytes())
}

func cqlWriteBytes(buf *bytes.Buffer, data []byte) {
	binary.Write(buf, binary.BigEndian, int32(len(data)))
	buf.Write(data)
}

func cqlShortString(buf *bytes.Buffer, s string) {
	binary.Write(buf, binary.BigEndian, uint16(len(s)))
	buf.WriteString(s)
}

func cqlWriteFrame(conn net.Conn, version, opcode byte, stream uint16, body []byte) {
	frame := make([]byte, 9+len(body))
	frame[0] = version
	frame[1] = 0x00
	binary.BigEndian.PutUint16(frame[2:4], stream)
	frame[4] = opcode
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(body)))
	copy(frame[9:], body)
	conn.Write(frame) //nolint:errcheck
}
