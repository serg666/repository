package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Router struct {
	Id  *int
	Key *string
}

type RouterSpecification interface {
	ToSqlClauses() string
}

type RouterRepository interface {
	Add(ctx interface{}, router *Router) error
	Delete(ctx interface{}, router *Router) (error, bool)
	Update(ctx interface{}, router *Router) (error, bool)
	Query(ctx interface{}, specification RouterSpecification) (error, int, []*Router)
}

type RouterWithoutSpecification struct {}

func (iws *RouterWithoutSpecification) ToSqlClauses() string {
	return ""
}

type RouterSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (iswlao *RouterSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", iswlao.limit, iswlao.offset)
}

type RouterSpecificationByID struct {
	id int
}

func (isbyid *RouterSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", isbyid.id)
}

type RouterSpecificationByKey struct {
	key string
}

func (isbyk *RouterSpecificationByKey) ToSqlClauses() string {
	return fmt.Sprintf("where key='%s'", isbyk.key)
}

func NewRouterSpecificationByID(id int) RouterSpecification {
	return &RouterSpecificationByID{id: id}
}

func NewRouterSpecificationByKey(key string) RouterSpecification {
	return &RouterSpecificationByKey{
		key: key,
	}
}

func NewRouterSpecificationWithLimitAndOffset(limit int, offset int) RouterSpecification {
	return &RouterSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

func NewRouterWithoutSpecification() RouterSpecification {
	return &RouterWithoutSpecification{}
}

type PGPoolRouterStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (is *PGPoolRouterStore) Add(ctx interface{}, router *Router) error {
	_, err := is.pool.Exec(
		context.Background(),
		"insert into routers (id, key) values ($1, $2)",
		router.Id,
		router.Key,
	)

	return err
}

func (is *PGPoolRouterStore) Delete(ctx interface{}, router *Router) (error, bool) {
	err := is.pool.QueryRow(
		context.Background(),
		"delete from routers where id=$1 returning key",
		router.Id,
	).Scan(
		&router.Key,
	)

	return err, err == pgx.ErrNoRows
}

func (is *PGPoolRouterStore) Query(ctx interface{}, specification RouterSpecification) (error, int, []*Router) {
	var l []*Router
	var c int = 0

	conn, err := is.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from routers",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get routers cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			"select id, key from routers %s",
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query routers rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var router Router

		if err = rows.Scan(
			&router.Id,
			&router.Key,
		); err != nil {
			return fmt.Errorf("failed to get router row: %v", err), c, l
		}
		l = append(l, &router)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of routers: %v", err), c, l
	}

	return nil, c, l
}

func (is *PGPoolRouterStore) Update(ctx interface{}, router *Router) (error, bool) {
	err := is.pool.QueryRow(
		context.Background(),
		`update routers set
			key=COALESCE($2, key)
		where id=$1 returning key`,
		router.Id,
		router.Key,
	).Scan(
		&router.Key,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolRouterStore(pool *pgxpool.Pool, logger LoggerFunc) RouterRepository {
	return &PGPoolRouterStore{
		pool:   pool,
		logger: logger,
	}
}
