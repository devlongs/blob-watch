package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// BlobTransaction represents a blob transaction (EIP-4844)
type BlobTransaction struct {
	Hash        common.Hash
	BlockNumber uint64
	From        string
	To          common.Address
	Value       *big.Int
	BlobSize    uint64
	Timestamp   time.Time
}

// DataCollector handles the collection of blob transactions
type DataCollector struct {
	client *ethclient.Client
}

// NewDataCollector creates a new DataCollector connected to an Ethereum node
func NewDataCollector(url string) (*DataCollector, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, err
	}
	return &DataCollector{client: client}, nil
}

// CollectBlobTransactions listens for new blocks and processes blob transactions
func (dc *DataCollector) CollectBlobTransactions(ctx context.Context) {
	headers := make(chan *types.Header)
	sub, err := dc.client.SubscribeNewHead(ctx, headers)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Listening for new blocks...")
	for {
		select {
		case err := <-sub.Err():
			log.Printf("Subscription error: %v", err)
			return
		case header := <-headers:
			block, err := dc.client.BlockByHash(ctx, header.Hash())
			if err != nil {
				log.Printf("Error retrieving block: %v", err)
				continue
			}
			dc.processBlobTransactions(ctx, block)
		}
	}
}

// processBlobTransactions processes each transaction in a block to check if it's a blob transaction
func (dc *DataCollector) processBlobTransactions(ctx context.Context, block *types.Block) {
	fmt.Printf("Processing block #%d\n", block.NumberU64())
	for _, tx := range block.Transactions() {
		if isBlobTransaction(tx) {
			blobTx := &BlobTransaction{
				Hash:        tx.Hash(),
				BlockNumber: block.NumberU64(),
				From:        getFromAddress(tx),
				To:          *tx.To(),
				Value:       tx.Value(),
				BlobSize:    tx.BlobGas(),
				Timestamp:   time.Unix(int64(block.Time()), 0),
			}
			printBlobTransaction(blobTx)
		}
	}
}

// isBlobTransaction checks if a transaction is a blob transaction (EIP-4844)
func isBlobTransaction(tx *types.Transaction) bool {
	return tx.Type() == types.BlobTxType // 0x03 for blob transactions
}

// getFromAddress gets the sender's address of a transaction
func getFromAddress(tx *types.Transaction) string {
	signer := types.NewEIP155Signer(tx.ChainId())
	from, _ := types.Sender(signer, tx)
	return from.Hex()
}

func printBlobTransaction(tx *BlobTransaction) {
	fmt.Printf("Blob Transaction:\n")
	fmt.Printf("  Tx Hash: %s\n", tx.Hash.Hex())
	fmt.Printf("  Block Number: %d\n", tx.BlockNumber)
	fmt.Printf("  From: %s\n", tx.From)
	fmt.Printf("  To: %s\n", tx.To.Hex())
	fmt.Printf("  Value: %s\n", tx.Value.String())
	fmt.Printf("  Blob Size: %d\n", tx.BlobSize)
	fmt.Printf("  Timestamp: %s\n", tx.Timestamp.String())
}

func main() {
	collector, err := NewDataCollector("wss://eth-sepolia.g.alchemy.com/v2/sUzWUlDLtJ1LnQCajoi2E8932RwNFuaY")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	collector.CollectBlobTransactions(ctx)
}
