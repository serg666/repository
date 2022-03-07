package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Channel struct {
	Id     *int
	TypeId *int
	Key    *string
}

func (c *Channel) String() string {
	return fmt.Sprintf("Channel <%s>", *c.Key)
}

type ChannelSpecification interface {
	ToSqlClauses() string
}

type ChannelRepository interface {
	Add(ctx interface{}, channel *Channel) error
	Delete(ctx interface{}, channel *Channel) (error, bool)
	Update(ctx interface{}, channel *Channel) (error, bool)
	Query(ctx interface{}, specification ChannelSpecification) (error, int, []*Channel)
}

type ChannelWithoutSpecification struct {}

func (cws *ChannelWithoutSpecification) ToSqlClauses() string {
	return ""
}

type ChannelSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (cswlao *ChannelSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", cswlao.limit, cswlao.offset)
}

type ChannelSpecificationByID struct {
	id int
}

func (csbyid *ChannelSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", csbyid.id)
}

type ChannelSpecificationByTypeID struct {
	typeId int
}

func (csbyti *ChannelSpecificationByTypeID) ToSqlClauses() string {
	return fmt.Sprintf("where type_id=%d", csbyti.typeId)
}

type ChannelSpecificationByKey struct {
	key string
}

func (csbyk *ChannelSpecificationByKey) ToSqlClauses() string {
	return fmt.Sprintf("where key='%s'", csbyk.key)
}

func NewChannelSpecificationByID(id int) ChannelSpecification {
	return &ChannelSpecificationByID{id: id}
}

func NewChannelSpecificationByTypeID(typeId int) ChannelSpecification {
	return &ChannelSpecificationByTypeID{
		typeId: typeId,
	}
}

func NewChannelSpecificationByKey(key string) ChannelSpecification {
	return &ChannelSpecificationByKey{
		key: key,
	}
}

func NewChannelSpecificationWithLimitAndOffset(limit int, offset int) ChannelSpecification {
	return &ChannelSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

func NewChannelWithoutSpecification() ChannelSpecification {
	return &ChannelWithoutSpecification{}
}

type PGPoolChannelStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (cs *PGPoolChannelStore) Add(ctx interface{}, channel *Channel) error {
	_, err := cs.pool.Exec(
		context.Background(),
		"insert into channels (id, type_id, key) values ($1, $2, $3)",
		channel.Id,
		channel.TypeId,
		channel.Key,
	)

	return err
}

func (cs *PGPoolChannelStore) Delete(ctx interface{}, channel *Channel) (error, bool) {
	err := cs.pool.QueryRow(
		context.Background(),
		"delete from channels where id=$1 returning type_id, key",
		channel.Id,
	).Scan(
		&channel.TypeId,
		&channel.Key,
	)

	return err, err == pgx.ErrNoRows
}

func (cs *PGPoolChannelStore) Query(ctx interface{}, specification ChannelSpecification) (error, int, []*Channel) {
	var l []*Channel
	var c int = 0

	conn, err := cs.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from channels",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get channels cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			"select id, type_id, key from channels %s",
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query channels rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var channel Channel

		if err = rows.Scan(
			&channel.Id,
			&channel.TypeId,
			&channel.Key,
		); err != nil {
			return fmt.Errorf("failed to get channel row: %v", err), c, l
		}
		l = append(l, &channel)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of channels: %v", err), c, l
	}

	return nil, c, l
}

func (cs *PGPoolChannelStore) Update(ctx interface{}, channel *Channel) (error, bool) {
	err := cs.pool.QueryRow(
		context.Background(),
		`update channels set
			type_id=COALESCE($2, type_id),
			key=COALESCE($3, key)
		where id=$1 returning type_id, key`,
		channel.Id,
		channel.TypeId,
		channel.Key,
	).Scan(
		&channel.TypeId,
		&channel.Key,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolChannelStore(pool *pgxpool.Pool, logger LoggerFunc) ChannelRepository {
	return &PGPoolChannelStore{
		pool:   pool,
		logger: logger,
	}
}
