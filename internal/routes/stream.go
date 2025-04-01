package routes

import (
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

const (
	chunkSize  = 10 * 1024 * 1024 // 10MB chunks
	bufferSize = 32 * 1024        // 32KB buffer for copy operations
)

var (
	log *zap.Logger
	errContentLengthExceeded = fmt.Errorf("content length exceeded")
)

type contentLengthWriter struct {
	http.ResponseWriter
	remaining int64
}

func (w *contentLengthWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errContentLengthExceeded
	}

	if int64(len(p)) > w.remaining {
		p = p[:w.remaining]
	}

	n, err := w.ResponseWriter.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "missing hash param", http.StatusBadRequest)
		return
	}

	worker := bot.GetNextWorker()

	file, err := utils.FileFromMessage(ctx, worker.Client, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

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

	// Generate ETag from file details
	eTag := fmt.Sprintf(`"%x-%x"`, file.ID, file.FileSize)
	ctx.Header("ETag", eTag)
	
	// Set caching headers (1 day cache)
	ctx.Header("Cache-Control", "public, max-age=86400")
	
	// Check If-None-Match header
	if match := r.Header.Get("If-None-Match"); match == eTag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// for photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    chunkSize,
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

	ctx.Header("Accept-Ranges", "bytes")
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
		start = ranges[0].Start
		end = ranges[0].End
		
		// Align range to chunk boundaries
		start = (start / chunkSize) * chunkSize
		if end-start > chunkSize {
			end = start + chunkSize - 1
		}
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

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"
	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}
	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	if r.Method != "HEAD" {
		lr, err := utils.NewTelegramReader(ctx, worker.Client, file.Location, start, end, chunkSize)
		if err != nil {
			log.Error("Error creating Telegram reader", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		cw := &contentLengthWriter{
			ResponseWriter: w,
			remaining:      contentLength,
		}

		buf := make([]byte, bufferSize)
		if _, err := io.CopyBuffer(cw, lr, buf); err != nil {
			if err != errContentLengthExceeded {
				log.Error("Error while copying stream", zap.Error(err))
			}
		}

		if cw.remaining > 0 {
			log.Warn("File content shorter than declared",
				zap.Int64("expected", contentLength),
				zap.Int64("actual", contentLength-cw.remaining),
			)
		}
	}
}
