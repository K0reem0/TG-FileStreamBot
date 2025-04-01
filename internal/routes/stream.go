package routes

import (
	"bytes"
	"compress/gzip"
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var (
	log          *zap.Logger
	clientPool   = make(chan *bot.Worker, 10) // Worker pool instead of raw client
	poolInitOnce sync.Once
)

type gzipResponseWriter struct {
	io.Writer
	gin.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

type bufferedTelegramReader struct {
	ctx       *gin.Context
	worker    *bot.Worker
	location  tg.InputFileLocationClass
	offset    int64
	fileSize  int64
	buffer    *bytes.Buffer
	chunkSize int64
	current   int64
}

func newBufferedTelegramReader(ctx *gin.Context, worker *bot.Worker, location tg.InputFileLocationClass, offset, length, chunkSize int64) *bufferedTelegramReader {
	return &bufferedTelegramReader{
		ctx:       ctx,
		worker:    worker,
		location:  location,
		offset:    offset,
		fileSize:  offset + length,
		buffer:    bytes.NewBuffer(make([]byte, 0, chunkSize)),
		chunkSize: chunkSize,
		current:   offset,
	}
}

func (b *bufferedTelegramReader) Read(p []byte) (n int, err error) {
	if b.buffer.Len() == 0 {
		if b.current >= b.fileSize {
			return 0, io.EOF
		}

		end := b.current + b.chunkSize
		if end > b.fileSize {
			end = b.fileSize
		}

		res, err := b.worker.Client.API().UploadGetFile(b.ctx, &tg.UploadGetFileRequest{
			Location: b.location,
			Offset:   b.current,
			Limit:    int(end - b.current),
		})
		if err != nil {
			return 0, err
		}

		result, ok := res.(*tg.UploadFile)
		if !ok {
			return 0, fmt.Errorf("unexpected response type")
		}

		b.buffer.Reset()
		if _, err := b.buffer.Write(result.GetBytes()); err != nil {
			return 0, err
		}

		b.current = end
	}

	return b.buffer.Read(p)
}

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	
	// Initialize worker pool
	poolInitOnce.Do(func() {
		for i := 0; i < cap(clientPool); i++ {
			clientPool <- bot.GetNextWorker()
		}
	})
	
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	// Get message ID
	messageID, err := strconv.Atoi(ctx.Param("messageID"))
	if err != nil {
		http.Error(w, "invalid message ID", http.StatusBadRequest)
		return
	}

	// Validate hash
	authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "missing hash param", http.StatusBadRequest)
		return
	}

	// Get worker from pool
	worker := <-clientPool
	defer func() { clientPool <- worker }()

	// Get file info
	file, err := utils.FileFromMessage(ctx, worker, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate hash
	expectedHash := utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	)
	if !utils.CheckHash(authHash, expectedHash) {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	}

	// Handle photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected response", http.StatusInternalServerError)
			return
		}
		fileBytes := result.GetBytes()
		ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		if r.Method != "HEAD" {
			ctx.Data(http.StatusOK, file.MimeType, fileBytes)
		}
		return
	}

	// Handle range requests
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		ranges, err := range_parser.Parse(file.FileSize, rangeHeader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(ranges) > 1 {
			http.Error(w, "multipart ranges not supported", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		if end >= file.FileSize {
			end = file.FileSize - 1
		}
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1
	mimeType := file.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// Set headers
	ctx.Header("Accept-Ranges", "bytes")
	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))
	ctx.Header("Cache-Control", "public, max-age=31536000, immutable")
	ctx.Header("ETag", expectedHash)

	// Content-Disposition
	disposition := "inline"
	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}
	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	// Video-specific optimizations
	if strings.HasPrefix(mimeType, "video/") {
		ctx.Header("X-Content-Type-Options", "nosniff")
		if rangeHeader == "" {
			ctx.Header("Content-Type", "video/mp4")
		}
	}

	if r.Method == "HEAD" {
		return
	}

	// Determine buffer size
	bufferSize := int64(256 * 1024) // 256KB default
	if strings.HasPrefix(mimeType, "video/") {
		bufferSize = 1 * 1024 * 1024 // 1MB for videos
	}

	// Create reader
	reader := newBufferedTelegramReader(ctx, worker, file.Location, start, contentLength, bufferSize)

	// Special handling for video streaming
	if strings.HasPrefix(mimeType, "video/") {
		flusher, ok := w.(http.Flusher)
		if ok {
			buf := make([]byte, bufferSize)
			for {
				n, err := reader.Read(buf)
				if err != nil && err != io.EOF {
					break
				}
				if n == 0 {
					break
				}
				if _, err := w.Write(buf[:n]); err != nil {
					break
				}
				flusher.Flush()
			}
			return
		}
	}

	// Compress non-media files
	if !strings.HasPrefix(mimeType, "video/") && 
	   !strings.HasPrefix(mimeType, "audio/") && 
	   !strings.HasPrefix(mimeType, "image/") {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = gzipResponseWriter{Writer: gz, ResponseWriter: w}
	}

	// Stream content
	if _, err := io.CopyN(w, reader, contentLength); err != nil {
		log.Error("Error while copying stream", zap.Error(err))
	}
}
