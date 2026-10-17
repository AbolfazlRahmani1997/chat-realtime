package ScyllaDB

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func clusterConfig(hosts string, port int, keyspace string, connectTimeout, timeout time.Duration, numConns int) *gocql.ClusterConfig {
	hostList := strings.Split(hosts, ",")
	for i := range hostList {
		hostList[i] = strings.TrimSpace(hostList[i])
	}

	cluster := gocql.NewCluster(hostList...)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.ConnectTimeout = connectTimeout
	cluster.Timeout = timeout
	cluster.NumConns = numConns
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.ReconnectInterval = 1 * time.Second
	cluster.WriteCoalesceWaitTime = 200 * time.Microsecond

	username := os.Getenv("SCYLLA_USERNAME")
	password := os.Getenv("SCYLLA_PASSWORD")
	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	// LocalOne is the fast path for RF=1 / single-DC chat writes.
	// Override with SCYLLA_CONSISTENCY=LOCAL_QUORUM when RF>=3.
	cluster.Consistency = parseConsistency(os.Getenv("SCYLLA_CONSISTENCY"))
	cluster.SerialConsistency = gocql.LocalSerial
	return cluster
}

func parseConsistency(v string) gocql.Consistency {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "ONE":
		return gocql.One
	case "LOCAL_QUORUM", "QUORUM":
		return gocql.LocalQuorum
	case "LOCAL_ONE", "":
		return gocql.LocalOne
	default:
		return gocql.LocalOne
	}
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
	numConns = 8
	if numConnsStr != "" {
		if v, err := strconv.Atoi(numConnsStr); err == nil && v > 0 {
			numConns = v
		}
	}

	return
}

func connect(hosts string, port int, keyspace string, connectTimeout, timeout time.Duration, numConns int) (*gocql.Session, error) {
	cluster := clusterConfig(hosts, port, keyspace, connectTimeout, timeout, numConns)
	return cluster.CreateSession()
}

// OpenSession connects, ensures schema, and returns a keyspace-bound session.
func OpenSession() (*gocql.Session, error) {
	hosts, port, keyspace, connectTimeout, timeout, numConns := parseConfig()

	bootstrap, err := connect(hosts, port, "", connectTimeout, timeout, numConns)
	if err != nil {
		return nil, fmt.Errorf("scylla bootstrap connect: %w", err)
	}
	EnsureSchema(bootstrap, keyspace)
	bootstrap.Close()

	session, err := connect(hosts, port, keyspace, connectTimeout, timeout, numConns)
	if err != nil {
		return nil, fmt.Errorf("scylla keyspace connect: %w", err)
	}
	log.Printf("ScyllaDB connected to %s:%d keyspace=%s numConns=%d", hosts, port, keyspace, numConns)
	return session, nil
}

// NewSession connects or fatals — used by server startup.
func NewSession() *gocql.Session {
	session, err := OpenSession()
	if err != nil {
		log.Fatalf("Failed to connect to ScyllaDB: %v", err)
	}
	return session
}

// EnsureSchema creates the unified access-pattern tables used by Hub + admin.
func EnsureSchema(session *gocql.Session, keyspace string) {
	if keyspace == "" {
		keyspace = "chat_ks"
	}

	stmts := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
			WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.messages (
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
		)`, keyspace),

		// Partition by room for cheap history reads (DESC clustering).
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.messages_by_room (
			room_id text,
			created_at timestamp,
			id text,
			content text,
			message_type text,
			username text,
			client_id text,
			uniqid text,
			PRIMARY KEY (room_id, created_at, id)
		) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)`, keyspace),

		// O(1) uniqid → id (avoids ALLOW FILTERING / SAI scan).
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.messages_by_uniqid (
			uniqid text PRIMARY KEY,
			id text
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_deliver (
			message_id text,
			client_id text,
			PRIMARY KEY (message_id, client_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_read (
			message_id text,
			client_id text,
			PRIMARY KEY (message_id, client_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_reactions (
			message_id text,
			user_id text,
			emoji text,
			room_id text,
			PRIMARY KEY (message_id, user_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_meta (
			message_id text PRIMARY KEY,
			forward_from text,
			edited boolean
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_secrets (
			message_id text PRIMARY KEY,
			audience set<text>
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.message_secret_views (
			message_id text,
			user_id text,
			PRIMARY KEY (message_id, user_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.rooms (
			id text PRIMARY KEY,
			name text,
			type text,
			status text,
			avatar_url text,
			created_at timestamp,
			updated_at timestamp
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.room_last_message (
			room_id text PRIMARY KEY,
			message_id text,
			content text,
			message_type text,
			username text,
			client_id text,
			created_at timestamp
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.room_members (
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
		)`, keyspace),

		// Partition by user — GetMyRooms without ALLOW FILTERING.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.rooms_by_user (
			user_id text,
			room_id text,
			pin boolean,
			room_type text,
			PRIMARY KEY (user_id, room_id)
		)`, keyspace),

		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.users (
			user_id text PRIMARY KEY,
			firstname text,
			lastname text,
			username text,
			avatar_url text,
			last_seen bigint
		)`, keyspace),
	}

	for _, q := range stmts {
		if err := session.Query(q).Exec(); err != nil {
			log.Printf("Scylla schema warning: %v", err)
		}
	}
	log.Println("ScyllaDB schema ensured")
}
