package server

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	defaultSegmentSize     = 64 * 1024 * 1024 // 64MB
	defaultSuffixCacheSize = 1 * 1024 * 1024  // 1MB
)

// Config holds parsed command-line configuration for a service.
type Config struct {
	Id            string
	Address       string
	ConfigFile    string
	WalDir        string
	ClusterUrls   map[string]string
	BatchInterval time.Duration
	Logger        *zap.Logger
}

// Factory creates a server from the given configuration.
type Factory func(address string, batchInterval time.Duration, config graft.Config) (*Server, error)

// RunServer runs a graft service with standard CLI flags and configuration.
// This handles all the boilerplate: flag parsing, config loading, WAL setup, and graceful shutdown.
//
// Usage:
//
//	func main() {
//	    server.RunServer("kvstore", service.NewKvServer)
//	}
func RunServer(serviceName string, factory Factory) {
	app := &cli.App{
		Name:  serviceName,
		Usage: fmt.Sprintf("Run a %s server node", serviceName),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "id",
				Usage:    "Node ID for this server (required)",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "service-addr",
				Usage: "HTTP address to listen on (e.g., :8001)",
			},
			&cli.StringFlag{
				Name:     "join",
				Usage:    "Cluster node addresses (e.g., n1=localhost:9001,n2=localhost:9002)",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "wal-dir",
				Usage: "WAL directory (defaults to 'log<id>')",
			},
			&cli.DurationFlag{
				Name:  "batch-interval",
				Usage: "Batch interval for commands",
			},
			&cli.StringFlag{
				Name:  "wal-segment-size",
				Value: "64MB",
				Usage: "WAL segment size (e.g., 64MB, 128MB)",
			},
			&cli.StringFlag{
				Name:  "wal-cache-size",
				Value: "1MB",
				Usage: "WAL suffix cache size (e.g., 1MB, 4MB)",
			},
			&cli.BoolFlag{
				Name:  "wal-mmap",
				Usage: "Enable memory-mapped WAL files",
			},
			&cli.StringFlag{
				Name:  "log-file",
				Usage: "Log output file (defaults to stderr)",
			},
		},
		Action: func(c *cli.Context) error {
			return runServer(c, factory)
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServer(c *cli.Context, factory Factory) error {
	id := c.String("id")
	walDir := c.String("wal-dir")

	if walDir == "" {
		walDir = "log" + id
	}

	segmentSize, err := parseSize(c.String("wal-segment-size"), defaultSegmentSize)
	if err != nil {
		return fmt.Errorf("invalid wal-segment-size: %w", err)
	}

	cacheSize, err := parseSize(c.String("wal-cache-size"), defaultSuffixCacheSize)
	if err != nil {
		return fmt.Errorf("invalid wal-cache-size: %w", err)
	}

	clusterUrls, err := infra.ParseAddressList(c.String("join"))
	if err != nil {
		return fmt.Errorf("parsing --join: %w", err)
	}

	if err := os.MkdirAll(walDir, 0755); err != nil {
		return fmt.Errorf("creating WAL directory: %w", err)
	}

	logger, err := buildLogger(c.String("log-file"))
	if err != nil {
		return fmt.Errorf("creating logger: %w", err)
	}
	defer logger.Sync()

	wal, err := graft.OpenWal(graft.WalOptions{
		Dir:             walDir,
		SegmentSize:     segmentSize,
		SuffixCacheSize: cacheSize,
		MemoryMapped:    c.Bool("wal-mmap"),
		Logger:          logger,
	})
	if err != nil {
		return fmt.Errorf("opening WAL: %w", err)
	}

	srv, err := factory(c.String("service-addr"), c.Duration("batch-interval"), graft.Config{
		Id:                    id,
		ClusterUrls:           clusterUrls,
		ElectionTimeoutMillis: graft.IntRange{Low: 150, High: 300},
		HeartbeatMillis:       50,
		Persistence:           wal,
		Logger:                logger,
	})
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	srv.Start()

	// Wait for interrupt signal for graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")
	if err := srv.Close(); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
	}

	return nil
}

func buildLogger(logFile string) (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	if logFile != "" {
		cfg.OutputPaths = []string{logFile}
		cfg.ErrorOutputPaths = []string{logFile}
	}
	return cfg.Build()
}

// parseSize parses a size string like "64MB", "1GB", or a plain number (bytes).
// Returns the default value if the string is empty.
func parseSize(s string, defaultVal int64) (int64, error) {
	if s == "" {
		return defaultVal, nil
	}

	s = strings.TrimSpace(strings.ToUpper(s))

	var multiplier int64 = 1
	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "K") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	}

	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}

	return val * multiplier, nil
}
