package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Instrument struct {
	Id  *int
	Key *string
}

type InstrumentSpecification interface {
	ToSqlClauses() string
}

type InstrumentRepository interface {
	Add(ctx interface{}, instrument *Instrument) error
	Delete(ctx interface{}, instrument *Instrument) (error, bool)
	Update(ctx interface{}, instrument *Instrument) (error, bool)
	Query(ctx interface{}, specification InstrumentSpecification) (error, int, []*Instrument)
}

type InstrumentWithoutSpecification struct {}

func (iws *InstrumentWithoutSpecification) ToSqlClauses() string {
	return ""
}

type InstrumentSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (iswlao *InstrumentSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", iswlao.limit, iswlao.offset)
}

type InstrumentSpecificationByID struct {
	id int
}

func (isbyid *InstrumentSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", isbyid.id)
}

type InstrumentSpecificationByKey struct {
	key string
}

func (isbyk *InstrumentSpecificationByKey) ToSqlClauses() string {
	return fmt.Sprintf("where key='%s'", isbyk.key)
}

func NewInstrumentSpecificationByID(id int) InstrumentSpecification {
	return &InstrumentSpecificationByID{id: id}
}

func NewInstrumentSpecificationByKey(key string) InstrumentSpecification {
	return &InstrumentSpecificationByKey{
		key: key,
	}
}

func NewInstrumentSpecificationWithLimitAndOffset(limit int, offset int) InstrumentSpecification {
	return &InstrumentSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

func NewInstrumentWithoutSpecification() InstrumentSpecification {
	return &InstrumentWithoutSpecification{}
}

type PGPoolInstrumentStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (is *PGPoolInstrumentStore) Add(ctx interface{}, instrument *Instrument) error {
	_, err := is.pool.Exec(
		context.Background(),
		"insert into instruments (id, key) values ($1, $2)",
		instrument.Id,
		instrument.Key,
	)

	return err
}

func (is *PGPoolInstrumentStore) Delete(ctx interface{}, instrument *Instrument) (error, bool) {
	err := is.pool.QueryRow(
		context.Background(),
		"delete from instruments where id=$1 returning key",
		instrument.Id,
	).Scan(
		&instrument.Key,
	)

	return err, err == pgx.ErrNoRows
}

func (is *PGPoolInstrumentStore) Query(ctx interface{}, specification InstrumentSpecification) (error, int, []*Instrument) {
	var l []*Instrument
	var c int = 0

	conn, err := is.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from instruments",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get instruments cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			"select id, key from instruments %s",
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query instruments rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var instrument Instrument

		if err = rows.Scan(
			&instrument.Id,
			&instrument.Key,
		); err != nil {
			return fmt.Errorf("failed to get instrument row: %v", err), c, l
		}
		l = append(l, &instrument)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of instruments: %v", err), c, l
	}

	return nil, c, l
}

func (is *PGPoolInstrumentStore) Update(ctx interface{}, instrument *Instrument) (error, bool) {
	err := is.pool.QueryRow(
		context.Background(),
		`update instruments set
			key=COALESCE($2, key)
		where id=$1 returning key`,
		instrument.Id,
		instrument.Key,
	).Scan(
		&instrument.Key,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolInstrumentStore(pool *pgxpool.Pool, logger LoggerFunc) InstrumentRepository {
	return &PGPoolInstrumentStore{
		pool:   pool,
		logger: logger,
	}
}
