package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/minio/minio-go/v7"
	"github.com/oklog/ulid/v2"
)

const maxUploadBytes int64 = 20 << 20

type uploadResponse struct {
	FileID   string `json:"file_id"`
	URL      string `json:"url"`
	MimeType string `json:"mime_type,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

func uploadDir() string {
	if v := strings.TrimSpace(os.Getenv("CHAT_UPLOAD_DIR")); v != "" {
		return v
	}
	return "./uploads"
}

func safeBaseName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "..", "")
	name = strings.ReplaceAll(name, string(os.PathSeparator), "")
	return name
}

// UploadFile stores a file (MinIO if configured, else local disk) and returns file_id.
// Multipart field name: "file".
func (h *Handler) UploadFile(c *gin.Context) {
	if _, err := h.getUser(c.Query("token")); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	fh, err := c.FormFile("file")
	if err != nil || fh == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file required"})
		return
	}
	if fh.Size <= 0 || fh.Size > maxUploadBytes {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "file too large"})
		return
	}

	src, err := fh.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot open file"})
		return
	}
	defer src.Close()

	var head [512]byte
	n, _ := io.ReadFull(src, head[:])
	sniff := http.DetectContentType(head[:n])

	ext := strings.ToLower(filepath.Ext(fh.Filename))
	if ext == "" {
		if exts, _ := mime.ExtensionsByType(sniff); len(exts) > 0 {
			ext = exts[0]
		}
	}
	if ext == "" {
		ext = ".bin"
	}

	id := ulid.Make().String()
	fileID := safeBaseName(id + ext)

	cl, bucket, useMinio, minioErr := minioEnsureClient()
	if minioErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "object storage unavailable: " + minioErr.Error()})
		return
	}

	if useMinio {
		key := minioObjectKey(fileID)
		reader := io.MultiReader(bytes.NewReader(head[:n]), src)
		var size int64 = -1
		if fh.Size > 0 {
			size = fh.Size
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		uploadInfo, err := cl.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{ContentType: sniff})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot upload to storage"})
			return
		}
		c.JSON(http.StatusOK, uploadResponse{
			FileID:   fileID,
			URL:      "/chat/files/" + fileID,
			MimeType: sniff,
			Size:     uploadInfo.Size,
		})
		return
	}

	// Local disk fallback
	dir := uploadDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot create upload dir"})
		return
	}
	dstPath := filepath.Join(dir, fileID)
	dst, err := os.Create(dstPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot save file"})
		return
	}
	defer dst.Close()

	if n > 0 {
		if _, err := dst.Write(head[:n]); err != nil {
			_ = os.Remove(dstPath)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot write file"})
			return
		}
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = os.Remove(dstPath)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot write file"})
		return
	}

	info, _ := dst.Stat()
	size := int64(0)
	if info != nil {
		size = info.Size()
	}

	c.JSON(http.StatusOK, uploadResponse{
		FileID:   fileID,
		URL:      "/chat/files/" + fileID,
		MimeType: sniff,
		Size:     size,
	})
}

// GetFile serves or redirects to a previously uploaded file by file_id.
func (h *Handler) GetFile(c *gin.Context) {
	fileID := safeBaseName(c.Param("fileId"))
	if fileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fileId required"})
		return
	}

	cl, bucket, useMinio, minioErr := minioEnsureClient()
	if minioErr != nil {
		fmt.Println(minioErr)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "object storage unavailable"})
		return
	}

	if useMinio {
		key := minioObjectKey(fileID)
		ctx, cancel := context.WithTimeout(context.Background(), minioConnectTimeout())
		defer cancel()
		obj, err := cl.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
			return
		}
		defer obj.Close()

		objInfo, err := obj.Stat()
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
			return
		}

		contentType := objInfo.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		c.Header("Content-Type", contentType)
		c.Header("Content-Disposition", "inline")
		c.DataFromReader(http.StatusOK, objInfo.Size, contentType, obj, nil)
		return
	}

	p := filepath.Join(uploadDir(), fileID)
	if st, err := os.Stat(p); err != nil || st == nil || st.IsDir() {
		c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
		return
	}
	c.File(p)
}

type fileMessageBody struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Mime string `json:"mime,omitempty"`
	Size int64  `json:"size,omitempty"`
}

func safeDisplayName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "file"
	}
	if utf8.RuneCountInString(name) > 120 {
		name = string([]rune(name)[:120])
	}
	return name
}

func uploadedFileExists(fileID string) bool {
	fileID = safeBaseName(fileID)
	if fileID == "" {
		return false
	}
	cl, bucket, useMinio, err := minioEnsureClient()
	if err != nil {
		return false
	}
	if useMinio {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, statErr := cl.StatObject(ctx, bucket, minioObjectKey(fileID), minio.StatObjectOptions{})
		return statErr == nil
	}
	st, statErr := os.Stat(filepath.Join(uploadDir(), fileID))
	return statErr == nil && st != nil && !st.IsDir()
}

// normalizeFileMessage accepts a file message body that points at an uploaded object.
func normalizeFileMessage(content string) (string, bool) {
	var body fileMessageBody
	if err := json.Unmarshal([]byte(strings.TrimSpace(content)), &body); err != nil {
		return "", false
	}
	id := strings.TrimPrefix(body.URL, "/chat/files/")
	if id == "" || strings.Contains(id, "/") || id != safeBaseName(id) {
		return "", false
	}
	if !uploadedFileExists(id) {
		return "", false
	}
	body.Name = safeDisplayName(body.Name)
	body.URL = "/chat/files/" + id
	if len(body.Mime) > 120 {
		body.Mime = body.Mime[:120]
	}
	if body.Size < 0 {
		body.Size = 0
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", false
	}
	return string(raw), true
}
