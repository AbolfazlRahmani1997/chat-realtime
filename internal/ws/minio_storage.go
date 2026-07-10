package ws

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var errMinIOAuth = errors.New("minio: MINIO_ACCESS_KEY and MINIO_SECRET_KEY required when MINIO_ENDPOINT is set")

// MinIO is optional. If MINIO_ENDPOINT is empty, uploads use local disk (existing behavior).
//
// Env:
//   MINIO_ENDPOINT       host:port (e.g. minio:9000) — no http:// prefix
//   MINIO_ACCESS_KEY
//   MINIO_SECRET_KEY
//   MINIO_BUCKET         default: chat-uploads
//   MINIO_USE_SSL        true / 1 to use HTTPS to MinIO
//   MINIO_REGION         optional, default ""
//   MINIO_OBJECT_PREFIX  optional prefix for object keys (e.g. chat/)
//   MINIO_PRESIGN_SECONDS optional presigned GET lifetime, default 86400
//   MINIO_CONNECT_TIMEOUT_SECONDS optional timeout for BucketExists/MakeBucket on init, default 120

var (
	minioMu       sync.RWMutex
	minioClient   *minio.Client
	minioBucket   string
	minioPrefix   string
	minioInitErr  error
	minioDisabled bool // true when MINIO_ENDPOINT not set
	minioInited   bool
)

func getMinIOPresignSeconds() time.Duration {
	s := strings.TrimSpace(os.Getenv("MINIO_PRESIGN_SECONDS"))
	if s == "" {
		return 24 * time.Hour
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(sec) * time.Second
}

func minioConnectTimeout() time.Duration {
	s := strings.TrimSpace(os.Getenv("MINIO_CONNECT_TIMEOUT_SECONDS"))
	if s == "" {
		return 120 * time.Second
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return 120 * time.Second
	}
	return time.Duration(sec) * time.Second
}

// normalizeMinIOEndpoint strips scheme and trailing slash. minio.New expects "host:port" only.
// If the URL started with https://, caller should enable TLS even when MINIO_USE_SSL is unset.
func normalizeMinIOEndpoint(raw string) (hostPort string, secureFromURL bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	low := strings.ToLower(raw)
	if strings.HasPrefix(low, "https://") {
		secureFromURL = true
		raw = raw[8:]
	} else if strings.HasPrefix(low, "http://") {
		raw = raw[7:]
	}
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	return raw, secureFromURL
}

// minioObjectKey returns the key used in the bucket (prefix + fileID).
func minioObjectKey(fileID string) string {
	p := strings.TrimSpace(minioPrefix)
	if p == "" {
		return fileID
	}
	p = strings.TrimRight(p, "/") + "/"
	return p + fileID
}

// minioEnsureClient lazily builds the MinIO client and bucket once.
func minioEnsureClient() (*minio.Client, string, bool, error) {
	minioMu.RLock()
	if minioInited {
		c, b, dis, err := minioClient, minioBucket, minioDisabled, minioInitErr
		minioMu.RUnlock()
		if dis {
			return nil, "", false, nil
		}
		if err != nil {
			return nil, "", false, err
		}
		return c, b, true, nil
	}
	minioMu.RUnlock()

	minioMu.Lock()
	defer minioMu.Unlock()
	if minioInited {
		if minioDisabled {
			return nil, "", false, nil
		}
		if minioInitErr != nil {
			return nil, "", false, minioInitErr
		}
		return minioClient, minioBucket, true, nil
	}

	rawEndpoint := os.Getenv("MINIO_ENDPOINT")
	endpoint, secureFromURL := normalizeMinIOEndpoint(rawEndpoint)
	if endpoint == "" {
		minioDisabled = true
		minioInited = true
		return nil, "", false, nil
	}

	access := os.Getenv("MINIO_ACCESS_KEY")
	secret := os.Getenv("MINIO_SECRET_KEY")
	if access == "" || secret == "" {
		minioInitErr = errMinIOAuth
		minioInited = true
		return nil, "", false, minioInitErr
	}

	bucket := strings.TrimSpace(os.Getenv("MINIO_BUCKET"))
	if bucket == "" {
		bucket = "chat-uploads"
	}
	minioPrefix = os.Getenv("MINIO_OBJECT_PREFIX")

	useSSL := secureFromURL ||
		strings.EqualFold(os.Getenv("MINIO_USE_SSL"), "true") ||
		os.Getenv("MINIO_USE_SSL") == "1"
	region := strings.TrimSpace(os.Getenv("MINIO_REGION"))

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: useSSL,
	}
	if region != "" {
		opts.Region = region
	}

	cl, err := minio.New(endpoint, opts)
	if err != nil {
		minioInitErr = err
		minioInited = true
		return nil, "", false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), minioConnectTimeout())
	defer cancel()

	exists, err := cl.BucketExists(ctx, bucket)
	if err != nil {
		minioInitErr = err
		minioInited = true
		return nil, "", false, err
	}
	if !exists {
		err = cl.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
		if err != nil {
			minioInitErr = err
			minioInited = true
			return nil, "", false, err
		}
	}

	minioClient = cl
	minioBucket = bucket
	minioInited = true
	return minioClient, minioBucket, true, nil
}
