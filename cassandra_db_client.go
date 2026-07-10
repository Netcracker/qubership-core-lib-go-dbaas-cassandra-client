package cassandradbaas

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gocql/gocql"
	dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
	"github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/cache"
	"github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client/v3/model"
	"github.com/netcracker/qubership-core-lib-go/v3/utils"
)

const (
	checkConnectionQuery = "select release_version from system.local"
)

type CassandraDbClient interface {
	GetSession(ctx context.Context) (*gocql.Session, error)
}

type cassandraDbClient struct {
	clusterConfig  *gocql.ClusterConfig
	dbaasClient    dbaasbase.DbaaSClient
	cassandraCache *cache.DbaaSCache
	params         model.DbParams
}

func (c *cassandraDbClient) GetSession(ctx context.Context) (*gocql.Session, error) {
	classifier := c.params.Classifier(ctx)
	key := cache.NewKey(DbType, classifier)
	sessionRaw, err := c.cassandraCache.Cache(key, c.createNewSession(ctx, classifier))
	if err != nil {
		return nil, err
	}
	session := sessionRaw.(*gocql.Session)
	if !c.isPasswordValid(ctx, session) {
		session.Close()
		c.cassandraCache.Delete(key)
		sessionRaw, err = c.cassandraCache.Cache(key, c.createNewSession(ctx, classifier))
		if err != nil {
			return nil, err
		}
		session = sessionRaw.(*gocql.Session)
		if err = waitForSessionReconnect(ctx, session, c.clusterConfig.ConnectTimeout); err != nil {
			return nil, err
		}
	}
	return session, nil
}

func waitForSessionReconnect(ctx context.Context, session *gocql.Session, waitTime time.Duration) (err error) {
	ctx, cancelContext := context.WithTimeout(ctx, waitTime)
	checkInterval := 100 * time.Millisecond
	defer cancelContext()
	err = session.Query(checkConnectionQuery).WithContext(ctx).Exec()
	for err != nil {
		select {
		case <-ctx.Done():
			return err
		case <-time.After(checkInterval):
			err = session.Query(checkConnectionQuery).WithContext(ctx).Exec()
		}
	}
	return err
}

func (c *cassandraDbClient) createNewSession(ctx context.Context, classifier map[string]interface{}) func() (interface{}, error) {
	return func() (interface{}, error) {
		logger.Debug("Create gocql session with classifier %+v", classifier)
		logicalDb, err := c.dbaasClient.GetOrCreateDb(ctx, DbType, classifier, c.params.BaseDbParams)
		if err != nil {
			return nil, err
		}
		keyspace := logicalDb.ConnectionProperties["keyspace"].(string)
		contactPointsSlice := logicalDb.ConnectionProperties["contactPoints"].([]interface{})
		port := int(logicalDb.ConnectionProperties["port"].(float64))
		username := logicalDb.ConnectionProperties["username"].(string)
		password := logicalDb.ConnectionProperties["password"].(string)
		contactPoints := make([]string, len(contactPointsSlice))
		for i, contactPoint := range contactPointsSlice {
			contactPoints[i] = contactPoint.(string)
		}
		if tls, ok := logicalDb.ConnectionProperties["tls"].(bool); ok && tls {
			logger.Infof("Connection to cassandra db with classifier %+v will be secured", classifier)
			c.clusterConfig.SslOpts = &gocql.SslOptions{
				Config: utils.GetTlsConfig(),
			}
		}

		c.clusterConfig.Hosts = contactPoints
		c.clusterConfig.Port = port
		c.clusterConfig.Keyspace = keyspace
		c.clusterConfig.Authenticator = &gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
		logger.Debug("Build gocql session for cassandra database with classifier %+v. Contact points: %s.", classifier, contactPoints)
		session, err := c.clusterConfig.CreateSession()
		if err != nil {
			logger.ErrorC(ctx, "Unable to create gocql session: %+v", err)
			return nil, err
		}
		return session, nil
	}
}

func (c *cassandraDbClient) getNewPassword(ctx context.Context, classifier map[string]interface{}) (string, error) {
	newConnection, err := c.dbaasClient.GetConnection(ctx, DbType, classifier, c.params.BaseDbParams)
	if err != nil {
		logger.ErrorC(ctx, "Can't update connection with dbaas")
		return "", err
	}
	if newPassword, ok := newConnection["password"]; ok {
		return newPassword.(string), nil
	}
	return "", errors.New("connection string doesn't contain password field")
}

func (c *cassandraDbClient) isPasswordValid(ctx context.Context, session *gocql.Session) bool {
	err := session.Query(checkConnectionQuery).WithContext(ctx).Exec()
	if err != nil {
		return !strings.Contains(err.Error(), "no hosts available in the pool")
	}
	return true
}
