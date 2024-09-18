package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	logger *zap.Logger
	db     *gorm.DB

	blobTransactionsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "blob_transactions_processed_total",
			Help: "The total number of blob transactions processed",
		},
	)
)

type BlobTransaction struct {
	ID          uint `gorm:"primaryKey"`
	Hash        string
	BlockNumber uint64
	From        string
	To          string
	Value       string
	BlobSize    uint64
	Timestamp   time.Time
}

type DataCollector struct {
	client *ethclient.Client
	db     *gorm.DB
}

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		logger.Fatal("Error reading config file", zap.Error(err))
	}

	db = initDB()

	prometheus.MustRegister(blobTransactionsProcessed)
}

func initDB() *gorm.DB {
	dsn := viper.GetString("database.url")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}

	err = db.AutoMigrate(&BlobTransaction{})
	if err != nil {
		logger.Fatal("Failed to migrate database", zap.Error(err))
	}

	return db
}

func NewDataCollector(url string) (*DataCollector, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, err
	}
	return &DataCollector{client: client, db: db}, nil
}

func (dc *DataCollector) CollectBlobTransactions(ctx context.Context) error {
	for {
		if err := dc.subscribe(ctx); err != nil {
			logger.Error("Subscription error, reconnecting", zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (dc *DataCollector) subscribe(ctx context.Context) error {
	headers := make(chan *types.Header)
	sub, err := dc.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	for {
		select {
		case err := <-sub.Err():
			return err
		case header := <-headers:
			if err := dc.processBlock(ctx, header); err != nil {
				logger.Error("Error processing block", zap.Error(err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (dc *DataCollector) processBlock(ctx context.Context, header *types.Header) error {
	block, err := dc.client.BlockByHash(ctx, header.Hash())
	if err != nil {
		return err
	}

	logger.Info("Processing block", zap.Uint64("number", block.NumberU64()))

	var wg sync.WaitGroup
	for _, tx := range block.Transactions() {
		if isBlobTransaction(tx) {
			wg.Add(1)
			go func(tx *types.Transaction) {
				defer wg.Done()
				if err := dc.processBlobTransaction(ctx, tx, block); err != nil {
					logger.Error("Error processing blob transaction", zap.Error(err))
				}
			}(tx)
		}
	}
	wg.Wait()

	return nil
}

func (dc *DataCollector) processBlobTransaction(ctx context.Context, tx *types.Transaction, block *types.Block) error {
	blobTx := BlobTransaction{
		Hash:        tx.Hash().Hex(),
		BlockNumber: block.NumberU64(),
		From:        getFromAddress(tx),
		To:          tx.To().Hex(),
		Value:       tx.Value().String(),
		BlobSize:    tx.BlobGas(),
		Timestamp:   time.Unix(int64(block.Time()), 0),
	}

	result := dc.db.Create(&blobTx)
	if result.Error != nil {
		return result.Error
	}

	blobTransactionsProcessed.Inc()
	logger.Info("Processed blob transaction", zap.String("hash", blobTx.Hash))

	return nil
}

func isBlobTransaction(tx *types.Transaction) bool {
	return tx.Type() == types.BlobTxType
}

func getFromAddress(tx *types.Transaction) string {
	signer := types.NewEIP155Signer(tx.ChainId())
	from, _ := types.Sender(signer, tx)
	return from.Hex()
}

func runHTTPServer(wg *sync.WaitGroup) {
	defer wg.Done()

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	port := viper.GetString("server.port")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: http.DefaultServeMux,
	}

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	logger.Info("HTTP server started", zap.String("port", port))
}

func main() {
	defer logger.Sync()

	ethURL := viper.GetString("ethereum.url")
	if ethURL == "" {
		logger.Fatal("Ethereum URL not set")
	}

	collector, err := NewDataCollector(ethURL)
	if err != nil {
		logger.Fatal("Failed to create data collector", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go runHTTPServer(&wg)

	go func() {
		if err := collector.CollectBlobTransactions(ctx); err != nil {
			logger.Error("Error collecting blob transactions", zap.Error(err))
			cancel()
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	logger.Info("Shutting down gracefully...")
	cancel()
	wg.Wait()
}
