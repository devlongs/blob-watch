package main

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func testDBConnection() {
	dsn := viper.GetString("database.url")
	logger.Info("Testing database connection", zap.String("dsn", dsn))

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}

	var result int
	err = db.Raw("SELECT 1").Scan(&result).Error
	if err != nil {
		logger.Fatal("Failed to execute test query", zap.Error(err))
	}

	logger.Info("Successfully connected to database and executed test query")
}
