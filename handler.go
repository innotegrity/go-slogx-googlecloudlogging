package slogxgooglecloudlogging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/logging"
	"go.innotegrity.dev/async"
	"go.innotegrity.dev/generic"
	"go.innotegrity.dev/slogx"
	"go.innotegrity.dev/slogx/formatter"
	"golang.org/x/exp/slog"
	"google.golang.org/api/option"
)

// GoogleCloudLoggingHandlerOptionsContext can be used to retrieve the options used by the handler from the context.
type GoogleCloudLoggingHandlerOptionsContext struct{}

// GoogleCloudLoggingHandlerOptions holds the options for the JSON handler.
type GoogleCloudLoggingHandlerOptions struct {
	// ClientOptions is a list of options for the Google Cloud Logging client.
	ClientOptions []option.ClientOption

	// EnableAsync will execute the Handle() function in a separate goroutine.
	//
	// When async is enabled, you should be sure to call the Shutdown() function or use the slogx.Shutdown()
	// function to ensure all goroutines are finished and any pending records have been written.
	EnableAsync bool

	// Level is the minimum log level to write to the handler.
	//
	// By default, the level will be set to slog.LevelInfo if not supplied.
	Level slog.Leveler

	// LevelMapper is a function to use to map an slog.Leveler level to the corresponding Google Cloud Logging severity.
	//
	// If nil, the default mapper will be used, which should be fine for most cases.
	LevelMapper func(slog.Leveler) logging.Severity

	// LoggerOptions is a list of options to pass to the Google Cloud Logging client's underlying logger.
	LoggerOptions []logging.LoggerOption

	// LogName is the name of the log to use when logging messages.
	//
	// This option is required.
	LogName string

	// ProjectID is the ID of the GCP project to which the logger belongs.
	//
	// This option is required.
	ProjectID string

	// RecordFormatter specifies the formatter to use to format the record before sending it to the GCP logger.
	//
	// You should always be sure to format the buffer into a proper JSON payload.
	//
	// If no formatter is supplied, formatters.DefaultJSONFormatter is used to format the output.
	RecordFormatter formatter.BufferFormatter
}

// DefaultGoogleCloudLoggingHandlerLevelMapper is a default function for mapping slog levels to GCP logging levels.
func DefaultGoogleCloudLoggingHandlerLevelMapper(level slog.Leveler) logging.Severity {
	switch slogx.Level(level.Level()) {
	case slogx.LevelTrace, slogx.LevelDebug:
		return logging.Debug
	case slogx.LevelInfo:
		return logging.Info
	case slogx.LevelNotice:
		return logging.Notice
	case slogx.LevelWarn:
		return logging.Warning
	case slogx.LevelError:
		return logging.Error
	case slogx.LevelFatal:
		return logging.Critical
	case slogx.LevelPanic:
		return logging.Emergency
	}
	return logging.Default
}

// googleCloudLoggingHandler is a log handler that writes records to Google Cloud Logging.
type googleCloudLoggingHandler struct {
	activeGroup string
	attrs       []slog.Attr
	client      *logging.Client
	futures     []async.Future
	groups      []string
	logger      *logging.Logger
	options     GoogleCloudLoggingHandlerOptions
}

// NewGoogleCloudLoggingHandler creates a new handler object.
func NewGoogleCloudLoggingHandler(opts GoogleCloudLoggingHandlerOptions) (*googleCloudLoggingHandler, error) {
	// validate required options
	if opts.LogName == "" {
		return nil, errors.New("log name is required and cannot be empty")
	}
	if opts.ProjectID == "" {
		return nil, errors.New("project ID is required and cannot be empty")
	}

	// set default options
	if opts.Level == nil {
		opts.Level = slog.LevelInfo
	}

	// create the handler
	fmt.Println("creating client...")
	client, err := logging.NewClient(context.Background(), opts.ProjectID, opts.ClientOptions...)
	if err != nil {
		return nil, err
	}
	return &googleCloudLoggingHandler{
		attrs:   []slog.Attr{},
		client:  client,
		logger:  client.Logger(opts.LogName, opts.LoggerOptions...),
		futures: []async.Future{},
		groups:  []string{},
		options: opts,
	}, nil
}

// Enabled determines whether or not the given level is enabled in this handler.
func (h googleCloudLoggingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.options.Level.Level()
}

// Handle actually handles posting the record to the HTTP listener.
//
// Any attributes duplicated between the handler and record, including within groups, are automaticlaly removed.
// If a duplicate is encountered, the last value found will be used for the attribute's value.
func (h *googleCloudLoggingHandler) Handle(ctx context.Context, r slog.Record) error {
	handlerCtx := context.WithValue(ctx, GoogleCloudLoggingHandlerOptionsContext{}, &h.options)
	if !h.options.EnableAsync {
		return h.handle(handlerCtx, r)
	}

	future := async.Exec(func() any {
		return h.handle(handlerCtx, r)
	})
	h.futures = append(h.futures, future)
	return nil
}

// Shutdown is responsible for cleaning up resources used by the handler.
func (h googleCloudLoggingHandler) Shutdown(continueOnError bool) error {
	for _, f := range h.futures {
		if f != nil {
			f.Await()
		}
	}
	if h.client != nil {
		h.client.Close()
	}
	return nil
}

// WithAttrs creates a new handler from the existing one adding the given attributes to it.
func (h googleCloudLoggingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandler := &googleCloudLoggingHandler{
		attrs:   h.attrs,
		client:  h.client,
		futures: h.futures,
		groups:  h.groups,
		logger:  h.logger,
		options: h.options,
	}
	if h.activeGroup == "" {
		newHandler.attrs = append(newHandler.attrs, attrs...)
	} else {
		newHandler.attrs = append(newHandler.attrs, slog.Group(h.activeGroup, generic.AnySlice(attrs)...))
		newHandler.activeGroup = h.activeGroup
	}
	return newHandler
}

// WithGroup creates a new handler from the existing one adding the given group to it.
func (h googleCloudLoggingHandler) WithGroup(name string) slog.Handler {
	newHandler := &googleCloudLoggingHandler{
		attrs:   h.attrs,
		client:  h.client,
		futures: h.futures,
		groups:  h.groups,
		logger:  h.logger,
		options: h.options,
	}
	if name != "" {
		newHandler.groups = append(newHandler.groups, name)
		newHandler.activeGroup = name
	}
	return newHandler
}

// handle is responsible for actually posting the message to the HTTP listener.
func (h googleCloudLoggingHandler) handle(ctx context.Context, r slog.Record) error {
	attrs := slogx.ConsolidateAttrs(h.attrs, h.activeGroup, r)

	// format the output into a buffer
	var buf *slogx.Buffer
	var err error
	if h.options.RecordFormatter != nil {
		buf, err = h.options.RecordFormatter.FormatRecord(ctx, r.Time, slogx.Level(r.Level), r.PC, r.Message,
			attrs)
	} else {
		f := formatter.DefaultJSONFormatter()
		buf, err = f.FormatRecord(ctx, r.Time, slogx.Level(r.Level), r.PC, r.Message, attrs)
	}
	if err != nil {
		return err
	}

	// log the message synchronously since we're potentially already wrapped in a goroutine
	var severity logging.Severity
	if h.options.LevelMapper != nil {
		severity = h.options.LevelMapper(r.Level)
	} else {
		severity = DefaultGoogleCloudLoggingHandlerLevelMapper(r.Level)
	}
	err = h.logger.LogSync(ctx, logging.Entry{
		Timestamp: r.Time,
		Severity:  severity,
		Payload:   json.RawMessage(buf.Bytes()),
	})
	return err
}
