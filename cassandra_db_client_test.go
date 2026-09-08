package cassandradbaas

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	"github.com/netcracker/qubership-core-lib-go/v3/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// initTlsForTestBinary generates a self-signed certificate, writes it to a
// temporary directory, points utils at that directory, enables TLS and forces
// utils.GetTlsConfig to load the certificate into memory.  The temporary
// directory is removed immediately afterwards: the loaded certificate is
// retained in memory by the utils package for the rest of the binary run.
//
// Must be called from TestMain before m.Run so that utils.configOnce fires with
// tlsEnabled=true. Any test that calls dbaasbase.NewDbaaSPool (→ NewDbaasRestClient
// → utils.GetClient → utils.GetTlsConfig) would otherwise freeze the config with no
// certificates, causing the TLS mock handshake to fail.
func initTlsForTestBinary() error {
	certPEM, keyPEM, err := buildDnsOnlyCert()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "cassandra-tls-init")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	for _, f := range []struct {
		name string
		data []byte
	}{
		{utils.TlsCrt, certPEM},
		{utils.TlsKey, keyPEM},
		{utils.CaCrt, certPEM}, // self-signed: cert doubles as its own CA
	} {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, 0o600); err != nil {
			return err
		}
	}
	if err := os.Setenv(utils.TlsPathEnv, dir); err != nil {
		return err
	}
	utils.SetTlsEnabled(true)
	cfg := utils.GetTlsConfig()
	if len(cfg.Certificates) == 0 {
		return fmt.Errorf("utils.GetTlsConfig loaded no certificates after TLS init")
	}
	return nil
}

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
	dbaasClient := &validationErrorDbaasClient{err: context.DeadlineExceeded}

	client := &cassandraDbClient{
		clusterConfig: gocql.NewCluster(),
		cassandraCache: &cache.DbaaSCache{
			LogicalDbCache: map[cache.Key]interface{}{key: session},
		},
		dbaasClient: dbaasClient,
		params:      model.DbParams{Classifier: classifierFn},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() { _, err := client.GetSession(ctx); resultCh <- err }()
	select {
	case err := <-resultCh:
		require.ErrorIs(suite.T(), err, context.DeadlineExceeded)
		require.Equal(suite.T(), 1, dbaasClient.getOrCreateCalls)
	case <-time.After(2 * time.Second):
		suite.T().Fatal("Context timeout is not taken into account by GetSession")
	}
}

type validationErrorDbaasClient struct {
	err              error
	getOrCreateCalls int
}

func (c *validationErrorDbaasClient) GetOrCreateDb(
	context.Context,
	string,
	map[string]interface{},
	rest.BaseDbParams,
) (*basemodel.LogicalDb, error) {
	c.getOrCreateCalls++
	return nil, c.err
}

func (c *validationErrorDbaasClient) GetConnection(
	context.Context,
	string,
	map[string]interface{},
	rest.BaseDbParams,
) (map[string]interface{}, error) {
	return nil, c.err
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
			q := strings.ToLower(strings.TrimSpace(cqlReadLongString(body)))
			switch {
			case strings.HasPrefix(q, "use "):
				// gocql sends USE whenever the cluster config carries a keyspace.
				// An empty Rows frame here would fail with a protocol error.
				cqlWriteSetKeyspaceFrame(conn, respVersion, stream, q[len("use "):])
			case strings.Contains(q, "system.local"):
				cqlWriteSystemLocalFrame(conn, respVersion, stream)
			default:
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
	binary.Write(&body, binary.BigEndian, int32(6))      // columns_count
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
	// rpc_address gives hosts rebuilt from ring discovery a valid connect address.
	// Without it gocql discards the host before it is ever dialled.
	cqlShortString(&body, "rpc_address")
	binary.Write(&body, binary.BigEndian, uint16(0x0010)) // inet
	binary.Write(&body, binary.BigEndian, int32(1))       // rows_count = 1
	binary.Write(&body, binary.BigEndian, int32(16))      // host_id: 16-byte UUID
	body.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	cqlWriteBytes(&body, []byte("datacenter1"))
	cqlWriteBytes(&body, []byte("rack1"))
	binary.Write(&body, binary.BigEndian, int32(4)) // tokens: empty list (4-byte length prefix)
	binary.Write(&body, binary.BigEndian, int32(0)) // element count = 0
	cqlWriteBytes(&body, []byte("org.apache.cassandra.dht.Murmur3Partitioner"))
	cqlWriteBytes(&body, net.ParseIP(tlsMockNodeAddress).To4()) // rpc_address
	cqlWriteFrame(conn, version, 0x08, stream, body.Bytes())
}

// cqlWriteSetKeyspaceFrame answers a USE statement with a SetKeyspace result.
func cqlWriteSetKeyspaceFrame(conn net.Conn, version byte, stream uint16, keyspace string) {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int32(3)) // kind = SetKeyspace
	cqlShortString(&body, strings.Trim(keyspace, `"`))
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

const (
	// tlsMockHostname is the contact point the session is opened with. gocql
	// resolves contact points through DNS before any TLS happens, so this has to be
	// a name that genuinely resolves on the machine running the test.
	// It is also the only name present in the mock server certificate SAN.
	tlsMockHostname = "localhost"
	// tlsMockNodeAddress is both the address the mock listens on and the rpc_address
	// it reports in system.local. gocql actually dials it, so it cannot be an
	// arbitrary IP: the TCP connection must succeed for the TLS name verification
	// to be reached at all.
	tlsMockNodeAddress = "127.0.0.1"
)

// gocql opens the control connection using the DNS contact point, but then rediscovers
// cluster members through system.local / system.peers, where nodes are identified by IP
// only. A HostInfo built from ring discovery has no hostname, so HostnameAndPort() falls
// back to the bare IP. When SslOptions carry no ServerName, gocql copies that IP into
// tls.Config.ServerName and crypto/tls verifies it against the certificate IP SANs
// instead of the DNS SANs. A service certificate carrying only a DNS SAN is therefore
// rejected, every per-node handshake fails, the pool stays empty and no session is built.
func TestCreateNewSession_TlsVerificationUsesContactPointHostname(t *testing.T) {
	setupTlsTestEnvironment(t)

	done := make(chan struct{})
	defer close(done)

	port := startTLSCQLMock(t, done)

	classifier := map[string]interface{}{"scope": "service"}
	client := &cassandraDbClient{
		clusterConfig: gocql.NewCluster(),
		dbaasClient:   &tlsDbaasClientStub{port: port},
		params: model.DbParams{
			Classifier: func(context.Context) map[string]interface{} { return classifier },
		},
	}
	// Initial host lookup must stay enabled: the ring discovery step is exactly what
	// replaces the DNS contact point with a bare IP and triggers the bug.
	client.clusterConfig.DisableInitialHostLookup = false
	client.clusterConfig.NumConns = 1
	client.clusterConfig.ConnectTimeout = 5 * time.Second
	client.clusterConfig.Timeout = 5 * time.Second

	sessionRaw, err := client.createNewSession(context.Background(), classifier)()
	require.NoError(t, err,
		"session must be created: the certificate has to be verified against the contact point hostname, not against the IP discovered from system.local")

	session, ok := sessionRaw.(*gocql.Session)
	require.True(t, ok, "createNewSession must return a *gocql.Session")
	t.Cleanup(session.Close)
	require.False(t, session.Closed(), "session must have at least one live connection in the pool")

	require.NotNil(t, client.clusterConfig.SslOpts,
		"SslOpts must be set for a logical db advertising tls=true")
	require.Equal(t, tlsMockHostname, client.clusterConfig.SslOpts.Config.ServerName,
		"ServerName must be pinned to the contact point; otherwise gocql substitutes the discovered node IP and hostname verification fails")
}

// TestTlsHandshake_EmptyServerNameFallsBackToDialedIp pins down the crypto/tls rule the
// fix relies on, independently of gocql internals: an empty ServerName makes the client
// verify the dialed address, and a certificate carrying only DNS SANs cannot satisfy an
// IP literal.
func TestTlsHandshake_EmptyServerNameFallsBackToDialedIp(t *testing.T) {
	setupTlsTestEnvironment(t)

	done := make(chan struct{})
	defer close(done)

	address := fmt.Sprintf("%s:%d", tlsMockNodeAddress, startTLSCQLMock(t, done))

	// Empty ServerName: crypto/tls derives it from the address, i.e. the IP literal.
	failing := utils.GetTlsConfig()
	require.Empty(t, failing.ServerName, "utils.GetTlsConfig must not pre-set ServerName")
	conn, err := tls.Dial("tcp", address, failing)
	if err == nil {
		conn.Close()
		t.Fatal("handshake unexpectedly succeeded: a DNS-only certificate must not validate against an IP literal")
	}
	require.Contains(t, err.Error(), "certificate",
		"the handshake must fail on certificate verification, not on transport")

	// Same certificate, same address, but verification is directed at the DNS name.
	working := utils.GetTlsConfig()
	working.ServerName = tlsMockHostname
	secured, err := tls.Dial("tcp", address, working)
	require.NoError(t, err, "handshake must succeed once ServerName matches the DNS SAN")
	require.NoError(t, secured.Close())
}

// tlsTestEnvOnce guards the shared TLS setup: utils caches its config in a package level
// sync.Once, so the certificate files are read exactly once per test binary.
var tlsTestEnvOnce sync.Once

// setupTlsTestEnvironment writes the certificate files utils.GetTlsConfig expects and
// switches the shared utils config into TLS mode. The load is forced while the files
// still exist, after which the temporary directory can be removed: everything utils
// needs is already held in memory.
func setupTlsTestEnvironment(t *testing.T) {
	t.Helper()
	tlsTestEnvOnce.Do(func() {
		certPEM, keyPEM := generateDnsOnlyCertificate(t)

		dir, err := os.MkdirTemp("", "cassandra-tls-test")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		require.NoError(t, os.WriteFile(filepath.Join(dir, utils.TlsCrt), certPEM, 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, utils.TlsKey), keyPEM, 0o600))
		// The certificate is self-signed, so it doubles as its own CA.
		require.NoError(t, os.WriteFile(filepath.Join(dir, utils.CaCrt), certPEM, 0o600))

		require.NoError(t, os.Setenv(utils.TlsPathEnv, dir))
		utils.SetTlsEnabled(true)

		require.NotNil(t, utils.GetTlsConfig(), "TLS config must load from the generated certificate files")
	})
}

// generateDnsOnlyCertificate builds a self-signed certificate whose SAN contains a DNS
// name and nothing else. The absent IP SAN is the point: it is what makes verification
// against a bare IP literal fail, exactly like the cert-manager issued service
// certificate does in a real cluster.
func generateDnsOnlyCertificate(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	certPEM, keyPEM, err := buildDnsOnlyCert()
	require.NoError(t, err)
	return certPEM, keyPEM
}

// buildDnsOnlyCert is the error-returning equivalent of generateDnsOnlyCertificate,
// usable from TestMain where *testing.T is not available.
func buildDnsOnlyCert() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cassandra-test-cn"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{tlsMockHostname},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// startTLSCQLMock starts the in-process CQL mock behind a TLS listener and returns its
// port. The server presents the very same key pair the client trusts, so the only thing
// that can break the handshake is the name being verified.
func startTLSCQLMock(t *testing.T, done <-chan struct{}) int {
	t.Helper()

	listener, err := net.Listen("tcp", tlsMockNodeAddress+":0")
	require.NoError(t, err)

	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: utils.GetTlsConfig().Certificates,
		MinVersion:   tls.VersionTLS12,
	})
	t.Cleanup(func() { tlsListener.Close() })

	var checkFailed atomic.Bool
	go func() {
		for {
			conn, acceptErr := tlsListener.Accept()
			if acceptErr != nil {
				return
			}
			go serveCQLMock(conn, done, false, &checkFailed)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port
}

// tlsDbaasClientStub returns a logical db that asks for a secured connection and
// advertises its contact point by DNS name, the way DBaaS does in a real cluster.
type tlsDbaasClientStub struct {
	port int
}

func (c *tlsDbaasClientStub) GetOrCreateDb(
	context.Context,
	string,
	map[string]interface{},
	rest.BaseDbParams,
) (*basemodel.LogicalDb, error) {
	return &basemodel.LogicalDb{
		ConnectionProperties: map[string]interface{}{
			"keyspace":      testContainerKeyspace,
			"contactPoints": []interface{}{tlsMockHostname},
			"port":          float64(c.port),
			"username":      testContainerUser,
			"password":      testContainerPassword,
			"tls":           true,
		},
	}, nil
}

func (c *tlsDbaasClientStub) GetConnection(
	context.Context,
	string,
	map[string]interface{},
	rest.BaseDbParams,
) (map[string]interface{}, error) {
	return map[string]interface{}{"password": testContainerPassword}, nil
}
