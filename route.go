package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type RouterSettings map[string]interface{}

type Route struct {
	Id         *int
	Profile    *Profile
	Instrument *Instrument
	Account    *Account
	Router     *Router
	Settings   *RouterSettings
}

type RouteSpecification interface {
	ToSqlClauses() string
}

type RouteRepository interface {
	Add(ctx interface{}, route *Route) error
	Delete(ctx interface{}, route *Route) (error, bool)
	Update(ctx interface{}, route *Route) (error, bool)
	Query(ctx interface{}, specification RouteSpecification) (error, int, []*Route)
}

type RouteSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (rswlao *RouteSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", rswlao.limit, rswlao.offset)
}

type RouteSpecificationByID struct {
	id int
}

func (rsbyid *RouteSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", rsbyid.id)
}

func NewRouteSpecificationByID(id int) RouteSpecification {
	return &RouteSpecificationByID{id: id}
}

func NewRouteSpecificationWithLimitAndOffset(limit int, offset int) RouteSpecification {
	return &RouteSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

type PGPoolRouteStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (rs *PGPoolRouteStore) Add(ctx interface{}, route *Route) error {
	return rs.pool.QueryRow(
		context.Background(),
		`insert into routes (
			profile_id,
			instrument_id,
			account_id,
			router_id,
			settings
		) values ($1, $2, $3, $4, $5) returning id`,
		route.Profile.Id,
		route.Instrument.Id,
		route.Account.Id,
		route.Router.Id,
		route.Settings,
	).Scan(&route.Id)
}

func (rs *PGPoolRouteStore) Query(ctx interface{}, specification RouteSpecification) (error, int, []*Route) {
	var l []*Route
	var c int = 0

	conn, err := rs.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from routes",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get routes cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			`select
				id,
				profile_id,
				instrument_id,
				account_id,
				router_id,
				settings
			from routes %s`,
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query routes rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		route := Route{
			Profile:    &Profile{},
			Instrument: &Instrument{},
			Account:    &Account{},
			Router:     &Router{},
		}

		if err = rows.Scan(
			&route.Id,
			&route.Profile.Id,
			&route.Instrument.Id,
			&route.Account.Id,
			&route.Router.Id,
			&route.Settings,
		); err != nil {
			return fmt.Errorf("failed to get route row: %v", err), c, l
		}
		l = append(l, &route)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of routes: %v", err), c, l
	}

	return nil, c, l
}

func (rs *PGPoolRouteStore) Delete(ctx interface{}, route *Route) (error, bool) {
	err := rs.pool.QueryRow(
		context.Background(),
		`delete from
			routes
		where
			id=$1
		returning
			profile_id,
			instrument_id,
			account_id,
			router_id,
			settings`,
		route.Id,
	).Scan(
		&route.Profile.Id,
		&route.Instrument.Id,
		&route.Account.Id,
		&route.Router.Id,
		&route.Settings,
	)

	return err, err == pgx.ErrNoRows
}

func (rs *PGPoolRouteStore) Update(ctx interface{}, route *Route) (error, bool) {
	err := rs.pool.QueryRow(
		context.Background(),
		`update routes set
			profile_id=COALESCE($2, profile_id),
			instrument_id=COALESCE($3, instrument_id),
			account_id=COALESCE($4, account_id),
			router_id=COALESCE($5, router_id),
			settings=COALESCE($6, settings)
		where
			id=$1
		returning
			profile_id,
			instrument_id,
			account_id,
			router_id,
			settings`,
		route.Id,
		route.Profile.Id,
		route.Instrument.Id,
		route.Account.Id,
		route.Router.Id,
		route.Settings,
	).Scan(
		&route.Profile.Id,
		&route.Instrument.Id,
		&route.Account.Id,
		&route.Router.Id,
		&route.Settings,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolRouteStore(pool *pgxpool.Pool, logger LoggerFunc) RouteRepository {
	return &PGPoolRouteStore{
		pool:   pool,
		logger: logger,
	}
}
