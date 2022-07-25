package redisearch_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/RediSearch/redisearch-go/v2/redisearch"
	"github.com/gomodule/redigo/redis"
)

// exemplifies the NewClient function
func ExampleNewClient() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "myIndex")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("date"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := redisearch.NewDocument("ExampleNewClient:doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("date", time.Now().Unix())

	// Index the document. The API accepts multiple documents at a time
	if err := c.Index([]redisearch.Document{doc}...); err != nil {
		log.Fatal(err)
	}

	// Wait for all documents to be indexed
	info, _ := c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	// Searching with limit and sorting
	docs, total, err := c.Search(redisearch.NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)
	// Output: ExampleNewClient:doc1 Hello world 1 <nil>

	// Drop the existing index
	c.Drop()
}

// RediSearch 2.0, marks the re-architecture of the way indices are kept in sync with the data.
// Instead of having to write data through the index (using the FT.ADD command),
// RediSearch will now follow the data written in hashes and automatically index it.
// The following example illustrates how to achieve it with the go client
func ExampleClient_CreateIndexWithIndexDefinition() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	c := redisearch.NewClientFromPool(pool, "products-from-hashes")

	// Create a schema
	schema := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextFieldOptions("name", redisearch.TextFieldOptions{Sortable: true})).
		AddField(redisearch.NewTextFieldOptions("description", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("price"))

	// IndexDefinition is available for RediSearch 2.0+
	// Create a index definition for automatic indexing on Hash updates.
	// In this example we will only index keys started by product:
	indexDefinition := redisearch.NewIndexDefinition().AddPrefix("product:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(schema, indexDefinition)

	// Get a vanilla connection and create 100 hashes
	vanillaConnection := pool.Get()
	for productNumber := 0; productNumber < 100; productNumber++ {
		vanillaConnection.Do("HSET", fmt.Sprintf("product:%d", productNumber), "name", fmt.Sprintf("product name %d", productNumber), "description", "product description", "price", 10.99)
	}

	// Wait for all documents to be indexed
	info, _ := c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	_, total, _ := c.Search(redisearch.NewQuery("description"))

	fmt.Printf("Total documents containing \"description\": %d.\n", total)
}

// The following example illustrates an index creation and deletion.
// By default, DropIndex() which is a wrapper for RediSearch FT.DROPINDEX does not delete the document hashes associated with the index.
// Setting the argument deleteDocuments to true deletes the hashes as well.
// Available since RediSearch 2.0
func ExampleClient_DropIndex() {

	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	c := redisearch.NewClientFromPool(pool, "products-from-hashes")

	// Create a schema
	schema := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextFieldOptions("name", redisearch.TextFieldOptions{Sortable: true})).
		AddField(redisearch.NewTextFieldOptions("description", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("price"))

	// IndexDefinition is available for RediSearch 2.0+
	// Create a index definition for automatic indexing on Hash updates.
	// In this example we will only index keys started by product:
	indexDefinition := redisearch.NewIndexDefinition().AddPrefix("product:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(schema, indexDefinition)

	// Get a vanilla connection and create 100 hashes
	vanillaConnection := pool.Get()
	for productNumber := 0; productNumber < 100; productNumber++ {
		vanillaConnection.Do("HSET", fmt.Sprintf("product:%d", productNumber), "name", fmt.Sprintf("product name %d", productNumber), "description", "product description", "price", 10.99)
	}

	// Wait for all documents to be indexed
	info, _ := c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	// Delete Index and Documents
	err := c.DropIndex(true)
	if err != nil {
		log.Fatal(err)
	}

}

// exemplifies the NewClientFromPool function
func ExampleNewClientFromPool() {
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	c := redisearch.NewClientFromPool(pool, "search-client-1")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("date"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := redisearch.NewDocument("ExampleNewClientFromPool:doc2", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("date", time.Now().Unix())

	// Index the document. The API accepts multiple documents at a time
	if err := c.Index([]redisearch.Document{doc}...); err != nil {
		log.Fatal(err)
	}

	// Searching with limit and sorting
	docs, total, err := c.Search(redisearch.NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)
	// Output: ExampleNewClientFromPool:doc2 Hello world 1 <nil>

	// Drop the existing index
	c.Drop()
}

//Example of how to establish an SSL connection from your app to the RedisAI Server
func ExampleNewClientFromPool_ssl() {
	// Consider the following helper methods that provide us with the connection details (host and password)
	// and the paths for:
	//     tls_cert - A a X.509 certificate to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted
	//     tls_key - A a X.509 private key to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted
	//	   tls_cacert - A PEM encoded CA's certificate file
	host, password := getConnectionDetails()
	tlsready, tls_cert, tls_key, tls_cacert := getTLSdetails()

	// Skip if we dont have all files to properly connect
	if tlsready == false {
		return
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(tls_cert, tls_key)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(tls_cacert)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	clientTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	// InsecureSkipVerify controls whether a client verifies the
	// server's certificate chain and host name.
	// If InsecureSkipVerify is true, TLS accepts any certificate
	// presented by the server and any host name in that certificate.
	// In this mode, TLS is susceptible to man-in-the-middle attacks.
	// This should be used only for testing.
	clientTLSConfig.InsecureSkipVerify = true

	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host,
			redis.DialPassword(password),
			redis.DialTLSConfig(clientTLSConfig),
			redis.DialUseTLS(true),
			redis.DialTLSSkipVerify(true),
		)
	}}

	c := redisearch.NewClientFromPool(pool, "search-client-1")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("date"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := redisearch.NewDocument("ExampleNewClientFromPool_ssl:doc3", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("date", time.Now().Unix())

	// Index the document. The API accepts multiple documents at a time
	if err := c.Index([]redisearch.Document{doc}...); err != nil {
		log.Fatal(err)
	}

	// Searching with limit and sorting
	docs, total, err := c.Search(redisearch.NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)

	// Drop the existing index
	c.Drop()
}

// The following example illustrates geospatial search using RediSearch.
// This examples maps to the Redis vanilla example showcased on https://redis.io/commands/georadius#examples.
// We'll start by adding two docs ( one for each city ) and then do a georadius search based on a starting point
// and 2 distinct radius:
// 1)- First query with 100KM radius centered at long,lat 15,37 that should only output the city named "Catania";
// 2)- Second query with 200KM radius centered at long,lat 15,37 that should output the cities named "Palermo" and "Catania";
func ExampleClient_Search() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "cityIndex")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("city")).
		AddField(redisearch.NewGeoField("location"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create the city docs
	// Note While Specifying location you should specify in following order -> longitude,latitude
	// Same look and feel as GEOADD https://redis.io/commands/geoadd
	// This example maps to https://redis.io/commands/geoadd#examples
	docPalermo := redisearch.NewDocument("doc:Palermo", 1.0)
	docPalermo.Set("name", "Palermo").
		Set("location", "13.361389,38.115556")

	docCatania := redisearch.NewDocument("doc:Catania", 1.0)
	docCatania.Set("name", "Catania").
		Set("location", "15.087269,37.502669")

	// Index the documents. The API accepts multiple documents at a time
	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, docPalermo, docCatania); err != nil {
		log.Fatal(err)
	}

	// Searching for 100KM radius should only output Catania
	docs, _, _ := c.Search(redisearch.NewQuery("*").AddFilter(
		redisearch.Filter{
			Field: "location",
			Options: redisearch.GeoFilterOptions{
				Lon:    15,
				Lat:    37,
				Radius: 100,
				Unit:   redisearch.KILOMETERS,
			},
		},
	).Limit(0, 2))

	fmt.Println("100KM Radius search from longitude 15 latitude 37")
	fmt.Println(docs[0])

	// Searching for 200KM radius should output Catania and Palermo
	docs, _, _ = c.Search(redisearch.NewQuery("*").AddFilter(
		redisearch.Filter{
			Field: "location",
			Options: redisearch.GeoFilterOptions{
				Lon:    15,
				Lat:    37,
				Radius: 200,
				Unit:   redisearch.KILOMETERS,
			},
		},
	).Limit(0, 2).SetSortBy("location", true))
	fmt.Println("200KM Radius search from longitude 15 latitude 37")
	fmt.Println(docs[0])
	fmt.Println(docs[1])

	// Output: 100KM Radius search from longitude 15 latitude 37
	// {doc:Catania 1 [] map[location:15.087269,37.502669 name:Catania]}
	// 200KM Radius search from longitude 15 latitude 37
	// {doc:Palermo 1 [] map[location:13.361389,38.115556 name:Palermo]}
	// {doc:Catania 1 [] map[location:15.087269,37.502669 name:Catania]}
}

func getConnectionDetails() (host string, password string) {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host = "localhost:6379"
	password = ""
	valuePassword, existsPassword := os.LookupEnv("REDISEARCH_TEST_PASSWORD")
	if exists && value != "" {
		host = value
	}
	if existsPassword && valuePassword != "" {
		password = valuePassword
	}
	return
}

func getTLSdetails() (tlsready bool, tls_cert string, tls_key string, tls_cacert string) {
	tlsready = false
	value, exists := os.LookupEnv("TLS_CERT")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_cert = value
	} else {
		return
	}
	value, exists = os.LookupEnv("TLS_KEY")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_key = value
	} else {
		return
	}
	value, exists = os.LookupEnv("TLS_CACERT")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_cacert = value
	} else {
		return
	}
	tlsready = true
	return
}
