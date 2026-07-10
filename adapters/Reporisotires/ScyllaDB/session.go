package ScyllaDB

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

func clusterConfig(hosts string, port int, keyspace string, connectTimeout, timeout time.Duration, numConns int) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.ConnectTimeout = connectTimeout
	cluster.Timeout = timeout
	cluster.NumConns = numConns

	username := os.Getenv("SCYLLA_USERNAME")
	password := os.Getenv("SCYLLA_PASSWORD")
	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	return cluster
}

func parseConfig() (hosts string, port int, keyspace string, connectTimeout, timeout time.Duration, numConns int) {
	hosts = os.Getenv("SCYLLA_HOSTS")
	if hosts == "" {
		hosts = "localhost"
	}

	portStr := os.Getenv("SCYLLA_PORT")
	port = 9042
	if portStr != "" {
		if v, err := strconv.Atoi(portStr); err == nil {
			port = v
		}
	}

	keyspace = os.Getenv("SCYLLA_KEYSPACE")
	if keyspace == "" {
		keyspace = "chat_ks"
	}

	connectTimeoutStr := os.Getenv("SCYLLA_CONNECT_TIMEOUT")
	connectTimeout = 5 * time.Second
	if connectTimeoutStr != "" {
		if v, err := time.ParseDuration(connectTimeoutStr); err == nil {
			connectTimeout = v
		}
	}

	timeoutStr := os.Getenv("SCYLLA_TIMEOUT")
	timeout = 5 * time.Second
	if timeoutStr != "" {
		if v, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = v
		}
	}

	numConnsStr := os.Getenv("SCYLLA_NUMCONNS")
	numConns = 4
	if numConnsStr != "" {
		if v, err := strconv.Atoi(numConnsStr); err == nil && v > 0 {
			numConns = v
		}
	}

	return
}

func connect(hosts string, port int, keyspace string, connectTimeout, timeout time.Duration, numConns int) *gocql.Session {
	cluster := clusterConfig(hosts, port, keyspace, connectTimeout, timeout, numConns)
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	return session
}

func NewSession() *gocql.Session {
	hosts, port, keyspace, connectTimeout, timeout, numConns := parseConfig()

	session := connect(hosts, port, "", connectTimeout, timeout, numConns)
	defer session.Close()

	EnsureSchema(session)

	newSession := connect(hosts, port, keyspace, connectTimeout, timeout, numConns)
	log.Printf("ScyllaDB connected to %s:%d/keyspace=%s", hosts, port, keyspace)
	return newSession
}

func EnsureSchema(session *gocql.Session) {
	keyspace := os.Getenv("SCYLLA_KEYSPACE")
	if keyspace == "" {
		keyspace = "chat_ks"
	}

	err := session.Query(fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create keyspace (may already exist): %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.messages (
			id text PRIMARY KEY,
			content text,
			message_type text,
			room_id text,
			username text,
			client_id text,
			uniqid text,
			delivered boolean,
			read boolean,
			created_at timestamp,
			updated_at timestamp
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create messages table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id text,
			created_at timestamp,
			id text,
			content text,
			message_type text,
			username text,
			client_id text,
			uniqid text,
			delivered boolean,
			read boolean,
			PRIMARY KEY (room_id, created_at, id)
		) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create messages_by_room table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.message_deliver (
			message_id text,
			client_id text,
			PRIMARY KEY (message_id, client_id)
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create message_deliver table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.message_read (
			message_id text,
			client_id text,
			PRIMARY KEY (message_id, client_id)
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create message_read table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.rooms (
			id text PRIMARY KEY,
			name text,
			type boolean,
			status text,
			created_at timestamp,
			updated_at timestamp
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create rooms table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.room_last_message (
			room_id text PRIMARY KEY,
			message_id text,
			content text,
			message_type text,
			username text,
			client_id text,
			created_at timestamp
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create room_last_message table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.room_members (
			room_id text,
			user_id text,
			roles set<text>,
			firstname text,
			lastname text,
			avatar_url text,
			notification boolean,
			pin boolean,
			last_seen bigint,
			PRIMARY KEY (room_id, user_id)
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create room_members table: %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS messages_by_uniqid ON %s.messages (uniqid)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Info: could not create index on messages.uniqid (may already exist): %v", err)
	}

	err = session.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.users (
			user_id int PRIMARY KEY,
			firstname text,
			lastname text,
			username text,
			avatar_url text,
			last_seen bigint
		)
	`, keyspace)).Exec()
	if err != nil {
		log.Printf("Warning: could not create users table: %v", err)
	}

	log.Println("ScyllaDB schema ensured")
}
