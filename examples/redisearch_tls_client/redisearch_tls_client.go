package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/gomodule/redigo/redis"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var (
	tlsCertFile   = flag.String("tls-cert-file", "redis.crt", "A a X.509 certificate to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted.")
	tlsKeyFile    = flag.String("tls-key-file", "redis.key", "A a X.509 privat ekey to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted.")
	tlsCaCertFile = flag.String("tls-ca-cert-file", "ca.crt", "A PEM encoded CA's certificate file.")
	host          = flag.String("host", "127.0.0.1:6379", "Redis host.")
	password      = flag.String("password", "", "Redis password.")
)

func exists(filename string) (exists bool) {
	exists = false
	info, err := os.Stat(filename)
	if os.IsNotExist(err) || info.IsDir() {
		return
	}
	exists = true
	return
}

/*
 * Example of how to establish an SSL connection from your app to the RedisAI Server
 */
func main() {
	flag.Parse()
	// Quickly check if the files exist
	if !exists(*tlsCertFile) || !exists(*tlsKeyFile) || !exists(*tlsCaCertFile) {
		fmt.Println("Some of the required files does not exist. Leaving example...")
		return
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(*tlsCertFile, *tlsKeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(*tlsCaCertFile)
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
		return redis.Dial("tcp", *host,
			redis.DialPassword(*password),
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
	doc := redisearch.NewDocument("doc1", 1.0)
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
	// Output: doc1 Hello world 1 <nil>
}
