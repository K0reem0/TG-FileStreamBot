package routes

import (
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/utils"
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

const (
	chunkSize       = 1 * 1024 * 1024 // 1MB chunks for better throughput
	preloadSize     = 5 * 1024 * 1024 // Preload first 5MB for faster start
	maxCacheAge     = 24 * time.Hour   // Cache control max age
	readBufferSize  = 512 * 1024       // 512KB read buffer
	writeBufferSize = 512 * 1024       // 512KB write buffer
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded optimized stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	// Enable HTTP/2 push and buffering
	w.Header().Set("X-Accel-Buffering", "yes")

	// Parse message ID
	messageID, err := strconv.Atoi(ctx.Param("messageID"))
	if err != nil {
		http.Error(w, "Invalid message ID", http.StatusBadRequest)
		return
	}

	// Validate auth hash
	authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "Missing hash parameter", http.StatusBadRequest)
		return
	}

	// Get Telegram worker
	worker := bot.GetNextWorker()

	// Fetch file info
	file, err := utils.FileFromMessage(ctx, worker.Client, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Verify hash
	expectedHash := utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	)
	if !utils.CheckHash(authHash, expectedHash) {
		http.Error(w, "Invalid hash", http.StatusBadRequest)
		return
	}

	// Handle photo messages (special case)
	if file.FileSize == 0 {
		handlePhotoMessage(ctx, worker.Client, file)
		return
	}

	// Parse range header
	var start, end int64
	rangeHeader := r.Header.Get("Range")

	if rangeHeader == "" {
		// Full file request
		start = 0
		end = file.FileSize - 1
		w.WriteHeader(http.StatusOK)
	} else {
		// Partial content request
		ranges, err := range_parser.Parse(file.FileSize, rangeHeader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		
		// Preload optimization - limit initial request size
		if end-start > preloadSize {
			end = start + preloadSize - 1
		}
		
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	contentLength := end - start + 1

	// Set headers for optimal streaming
	setStreamingHeaders(ctx, file, contentLength)

	// Skip body for HEAD requests
	if r.Method == "HEAD" {
		return
	}

	// Create buffered reader and writer
	bufferedWriter := bufio.NewWriterSize(w, writeBufferSize)
	defer bufferedWriter.Flush()

	// Create Telegram reader with buffer
	lr, err := utils.NewTelegramReader(ctx, worker.Client, file.Location, start, end, contentLength)
	if err != nil {
		log.Error("Failed to create reader", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	bufferedReader := bufio.NewReaderSize(lr, readBufferSize)

	// Stream with large buffer
	if _, err := io.CopyN(bufferedWriter, bufferedReader, contentLength); err != nil {
		log.Error("Streaming error", zap.Error(err))
	}
}

func handlePhotoMessage(ctx *gin.Context, client *tg.Client, file *utils.FileInfo) {
	w := ctx.Writer
	r := ctx.Request

	res, err := client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
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
		http.Error(w, "Unexpected response", http.StatusInternalServerError)
		return
	}

	fileBytes := result.GetBytes()
	ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
	ctx.Header("Cache-Control", fmt.Sprintf("public, max-age=%d", int(maxCacheAge.Seconds())))

	if r.Method != "HEAD" {
		ctx.Data(http.StatusOK, file.MimeType, fileBytes)
	}
}

func setStreamingHeaders(ctx *gin.Context, file *utils.FileInfo, contentLength int64) {
	w := ctx.Writer
	r := ctx.Request

	// Common headers
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(maxCacheAge.Seconds())))

	// Content-Type
	mimeType := file.MimeType
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mimeType)

	// Content-Disposition
	disposition := "inline"
	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	// HTTP/2 Push for known video formats
	if strings.HasPrefix(mimeType, "video/") && r.ProtoMajor >= 2 {
		w.Header().Set("Link", "</next-segment>; rel=preload; as=fetch")
	}
}
