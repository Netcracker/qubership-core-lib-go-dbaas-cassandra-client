[![Go build](https://github.com/Netcracker/qubership-core-lib-go-dbaas-cassandra-client/actions/workflows/go-build.yml/badge.svg)](https://github.com/Netcracker/qubership-core-lib-go-dbaas-cassandra-client/actions/workflows/go-build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-dbaas-cassandra-client)

# Cassandra dbaas go client

This module provides convenient way of interaction with **cassandra** databases provided by dbaas-aggregator.
`Cassandra dbaas go client` supports _multi-tenancy_ and can work with both _service_ and _tenant_ databases.

- [Install](#install)
- [Usage](#usage)
    * [CassandraDbClient](#cassandradbclient)
    * [Cassandra multiusers](#cassandra-multiusers)
- [Classifier](#classifier)
- [SSL/TLS support](#ssltls-support)
- [Quick example](#quick-example)

## Install

To get cassandra dbaas client do:
```go
    go get github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client
```

## Usage

At first, it's necessary to register security implemention - dummy or your own, the followning example shows registration of required services:
```go
import (
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
)

func init() {
	serviceloader.Register(1, &security.DummyToken{})
}
```

Then the user should create `DbaaSCassandraClient`. This is a base client, which allows working with tenant and service databases.
To create instance of `DbaaSCassandraClient` use `NewClient(pool *dbaasbase.DbaaSPool) *DbaaSCassandraClient`.

Note that client has parameter _pool_. `dbaasbase.DbaaSPool` is a tool which stores all cached connections and
create new ones. To find more info visit [dbaasbase](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main/README.md)

Example of client creation:
```go
pool := dbaasbase.NewDbaasPool()
client := cassandradbaas.NewClient(pool)
```

_Note_:By default, `Cassandra dbaas go client` supports dbaas-aggregator as databases source. But there is a possibility for user to provide another
sources. To do so use [LogcalDbProvider](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main/README.md#logicaldbproviders)
from dbaasbase.

Next step is to create `Database` object. It just an interface which allows creating cassandra client.
At this step user may choose which type of database he will work with:  `service` or `tenant`.

* To work with service databases use `ServiceDatabase(params ...model.DbParams) Database`
* To work with tenant databases use `TenantDatabase(params ...model.DbParams) Database`

Each func has `DbParams` as parameter.

DbParams store information for database creation. Note that this parameter is optional, but if user doesn't pass Classifier,
default one will be used. More about classifiers [here](#classifier)

| Name           | Description                                                                                       | type                                                                                                                       |
|----------------|---------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Classifier     | function which builds classifier from context. Classifier should be unique for each cassandra db. | func(ctx context.Context) map[string]interface{}                                                                           |
| BaseDbParams   | Specific parameters for database operations.                                                      | [BaseDbParams](https://github.com/netcracker/qubership-core-lib-go-dbaas-base-client/blob/main#basedbparams)   |

Example how to create an instance of Database.
```go
 dbPool := dbaasbase.NewDbaasPool()
 client := cassandradbaas.NewClient(dbPool)
 serviceDB := client.ServiceDatabase() // service Database creation 
 tenantDB := client.TenantDatabase() // tenant Database creation 
```

`Database` allows:   
* get CassandraDbClient, through which you can create cassandra db and get `*gocql.Session` for database operation. 
`serviceDB` and `tenantDB`  instances should be singleton and it's enough to create them only once.

### CassandraDbClient

CassandraDbClient is a special object, which allows getting `*gocql.Session` to establish connection and to operate with a database. 
`CassandraDbClient` is a singleton and should be created only once.

CassandraDbClient has method `GetSession(ctx context.Context) (*gocql.Session, error)` which will return `*gocql.Session` to work with the database.
We strongly recommend not to store `*gocql.Session` as singleton and get new connection for every block of code.
This is because the password in the database may be changed (by dbaas or someone else) and then the connection will return an error. Every time the function
`cassandraDbClient.GetSession(ctx)`is called, the password lifetime and correctness is checked. If necessary, the password is updated.

_Note_: classifier will be created with context and function from DbParams.

To create cassandraDbClient use `GetCassandraClient(config ...*gocql.ClusterConfig) (CassandraDbClient, error)`

Parameters:
* config _optional_ - user may pass desired gocql.ClusterConfig or don't pass anything at all. Note that user **doesn't have to 
set connection parameters** with config, because these parameters will be received from dbaas-aggregator.

```go
    ctx := ctxmanager.InitContext(context.Background(), propagateHeaders()) // preferred way
    // ctx := context.Background() // also possible for service client, but not recommended
    config := gocql.NewCluster()
    config.ConnectTimeout = 1 * time.Second
    cassandraClient, err := database.GetCassandraClient(config) // with config
    session, err := cassandraClient.GetSession(ctx)

    var cassandraVersion string
    iter := session.Query("SELECT release_version FROM system.local").Iter()
    iter.Scan(&cassandraVersion)
    err := iter.Close()
    if err != nil { return err }
```

### Cassandra multiusers
For specifying connection properties user role you should add this role in BaseDbParams structure:

```go
params := model.DbParams{
        Classifier:   Classifier, //database classifier
        BaseDbParams: rest.BaseDbParams{Role: "admin"}, //for example "admin", "rw", "ro"
    }
dbPool := dbaasbase.NewDbaaSPool()
cassandraClient := cassandradbaas.NewClient(dbPool)
serviceDb := dbaasCassandraClient.ServiceDatabase(params) //or for tenant database - TenantDatabase(params)
cassandraClient, err := serviceDb.GetCassandraClient()
session, err := cassandraClient.GetSession(ctx)
```
Requests to DbaaS will contain the role you specify in this structure.

## Classifier

Classifier and dbType should be unique combination for each database. Fields "tenantId" or "scope" must be into users' custom classifiers.

User can use default service or tenant classifier. It will be used if user doesn't specify Classifier in DbParams. 
This is recommended approach, and we don't recommend using custom classifier because it can lead to some problems. 
Use can be reasonable if you migrate to this module and before used custom and not default classifier.


Default service classifier looks like:
```json
{
    "scope": "service",
    "microserviceName": "<ms-name>"
}
```

Default tenant classifier looks like

```json
{
  "scope": "tenant",
  "tenantId": "<tenant-external-id>",
  "microserviceName": "<ms-name>"
}
```
Note, that if user doesn't set `MICROSERVICE_NAME` (or `microservice.name`) property, there will be panic during default classifier creation.
Also, if there are no tenantId in tenantContext, **panic will be thrown**.

## SSL/TLS support

This library supports work with secured connections to cassandra. Connection will be secured if TLS mode is enabled in
cassandra-adapter.

For correct work with secured connections, the library requires having a truststore with certificate.
It may be public cloud certificate, cert-manager's certificate or any type of certificates related to database.
We do not recommend use self-signed certificates. Instead, use default NC-CA.

To start using TLS feature user has to enable it on the physical database (adapter's) side and add certificate to service truststore.

### Physical database switching
To enable TLS support in physical database redeploy cassandra with mandatory parameters
```yaml
tls.enabled=true;
```

In case of using cert-manager as certificates source add extra parameters
```yaml
tls.generateCerts.enabled=true;
tls.generateCerts.clusterIssuerName=<cluster issuer name>;
```

ClusterIssuerName identifies which Certificate Authority cert-manager will use to issue a certificate.
It can be obtained from the person in charge of the cert-manager on the environment.

### Add certificate to service truststore

The platform deployer provides the bulk uploading of certificates to truststores.

In order to add required certificates to services truststore:
1. Check and get certificate which is used in cassandra.
   * In most cases certificate is located in `Secrets` -> `root-ca` -> `ca.crt`
2. Create ticket to `PSUPCDO/Configuration` and ask DevOps team to add this certificate to your deployer job.
3. After that all new deployments via configured deployer will include new certificate. Deployer creates a secret with certificate.
   Make sure the certificate is mount into your microservice.
   On bootstrapping microservice there is generated truststore with default location and password.

## Quick example

Here we create cassandra tenant client, then get CassandraClient and execute a query.

application.yaml
```yaml
  microservice.name=sample-microservice
```

```go
package main

import (
	"context"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
    dbaasbase "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3"
    "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model"
    "github.com/netcracker/qubership-core-lib-go-dbaas-base-client/v3/model/rest"
	cassandradbaas "github.com/netcracker/qubership-core-lib-go-dbaas-cassandra-client"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseprovider/tenant"
    "github.com/gocql/gocql"
)

var logger logging.Logger

func init() {
	configloader.InitWithSourcesArray(configloader.BasePropertySources())
	logger = logging.GetLogger("main")
	ctxmanager.Register([]ctxmanager.ContextProvider{tenant.TenantProvider{}})
}

func main() {
	
	// some context initialization
	ctx := ctxmanager.InitContext(context.Background(), map[string]interface{}{tenant.TenantHeader: "123"})
	
	// cassandra service client creation
	dbPool := dbaasbase.NewDbaaSPool()
	dbaasCassandraClient := cassandradbaas.NewClient(dbPool)
	tenantDb := dbaasCassandraClient.TenantDatabase()

	// create cassandra client
	cassandraClient, err := tenantDb.GetCassandraClient() // singleton for tenant db. This object must be used to get connection in the entire application.
	session, err := cassandraClient.GetSession(ctx) // now we can receive *gocql.Session
	if err != nil {
		logger.Error("Error during cassandra session creation")
	}
	logger.Info("Got cassandra session %+v", session)

    var cassandraVersion string
    iter := session.Query("SELECT release_version FROM system.local").Iter()
    iter.Scan(&cassandraVersion)
    err := iter.Close()
    if err != nil {
    	logger.Error("Error retrieving cassandra version") 
    }
    logger.Info("Cassandra database version: %s", cassandraVersion)
}
```
