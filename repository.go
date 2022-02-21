package repository

import (
	"log"
	"fmt"
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
)

type LoggerFunc func(interface{}) *log.Logger

func MakePgPoolFromDSN(dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("Can not parse pg config: %v", err)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("Can not connect to pg: %v", err)
	}

	return pool, nil
}
