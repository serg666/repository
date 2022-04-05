package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

const (
	AUTH = "authorize"
	PREAUTH = "preauthorize"
	CONFIRMAUTH = "confirmauth"
	REVERSAL = "reversal"
	REFUND = "refund"
	REBILL = "rebill"

	NEW = "new"
	SUCCESS = "success"
	DECLINED = "declined"
	WAIT3DS = "wait3ds"
	WAITMETHODURL = "waitmethodurl"
)

type LoggerFunc func(interface{}) logrus.FieldLogger

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
