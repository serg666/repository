package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type RouterSettings map[string]interface{}

type Route struct {
	Id         *int            `json:"id"`
	Profile    *Profile        `json:"profile"`
	Instrument *Instrument     `json:"instrument"`
	Account    *Account        `json:"account"`
	Router     *Router         `json:"router"`
	Settings   *RouterSettings `json:"settings"`
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

type RouteSpecificationByProfileAndInstrument struct {
	profile    *Profile
	instrument *Instrument
}

func (rsbypai *RouteSpecificationByProfileAndInstrument) ToSqlClauses() string {
	return fmt.Sprintf("where profile_id=%d and instrument_id=%d", *rsbypai.profile.Id, *rsbypai.instrument.Id)
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

func NewRouteSpecificationByProfileAndInstrument(profile *Profile, instrument *Instrument) RouteSpecification {
	return &RouteSpecificationByProfileAndInstrument{
		profile:    profile,
		instrument: instrument,
	}
}

type PGPoolRouteStore struct {
	pool            *pgxpool.Pool
	profileStore    ProfileRepository
	instrumentStore InstrumentRepository
	accountStore    AccountRepository
	routerStore     RouterRepository
	logger          LoggerFunc
}

func (rs *PGPoolRouteStore) Add(ctx interface{}, route *Route) error {
	var profileId    *int
	var instrumentId *int
	var accountId    *int
	var routerId     *int

	if route.Profile != nil {
		profileId = route.Profile.Id
	}

	if route.Instrument != nil {
		instrumentId = route.Instrument.Id
	}

	if route.Account != nil {
		accountId = route.Account.Id
	}

	if route.Router != nil {
		routerId = route.Router.Id
	}

	return rs.pool.QueryRow(
		context.Background(),
		`insert into routes (
			profile_id,
			instrument_id,
			account_id,
			router_id,
			settings
		) values ($1, $2, $3, $4, $5) returning id`,
		profileId,
		instrumentId,
		accountId,
		routerId,
		route.Settings,
	).Scan(&route.Id)
}

func (rs *PGPoolRouteStore) refreshRouteProfile(ctx interface{}, route *Route) error {
	if !(route.Profile != nil && route.Profile.Id != nil) {
		return nil
	}

	err, _, profiles := rs.profileStore.Query(ctx, NewProfileSpecificationByID(
		*route.Profile.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update route profile: %v", err)
	}

	for _, profile := range profiles {
		route.Profile = profile
	}

	return nil
}

func (rs *PGPoolRouteStore) refreshRouteInstrument(ctx interface{}, route *Route) error {
	if !(route.Instrument != nil && route.Instrument.Id != nil) {
		return nil
	}

	err, _, instruments := rs.instrumentStore.Query(ctx, NewInstrumentSpecificationByID(
		*route.Instrument.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update route instrument: %v", err)
	}

	for _, instrument := range instruments {
		route.Instrument = instrument
	}

	return nil
}

func (rs *PGPoolRouteStore) refreshRouteAccount(ctx interface{}, route *Route) error {
	if !(route.Account != nil && route.Account.Id != nil) {
		return nil
	}

	err, _, accounts := rs.accountStore.Query(ctx, NewAccountSpecificationByID(
		*route.Account.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update route account: %v", err)
	}

	for _, account := range accounts {
		route.Account = account
	}

	return nil
}

func (rs *PGPoolRouteStore) refreshRouteRouter(ctx interface{}, route *Route) error {
	if !(route.Router != nil && route.Router.Id != nil) {
		return nil
	}

	err, _, routers := rs.routerStore.Query(ctx, NewRouterSpecificationByID(
		*route.Router.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update route router: %v", err)
	}

	for _, router := range routers {
		route.Router = router
	}

	return nil
}

func (rs *PGPoolRouteStore) refreshRouteForeigns(ctx interface{}, route *Route) error {
	if err := rs.refreshRouteProfile(ctx, route); err != nil {
		return err
	}

	if err := rs.refreshRouteInstrument(ctx, route); err != nil {
		return err
	}

	if err := rs.refreshRouteAccount(ctx, route); err != nil {
		return err
	}

	if err := rs.refreshRouteRouter(ctx, route); err != nil {
		return err
	}

	return nil
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
		var route Route
		var profileId *int
		var instrumentId *int
		var accountId *int
		var routerId *int

		if err = rows.Scan(
			&route.Id,
			&profileId,
			&instrumentId,
			&accountId,
			&routerId,
			&route.Settings,
		); err != nil {
			return fmt.Errorf("failed to get route row: %v", err), c, l
		}
		if profileId != nil {
			route.Profile = &Profile{
				Id: profileId,
			}
		}
		if instrumentId != nil {
			route.Instrument = &Instrument{
				Id: instrumentId,
			}
		}
		if accountId != nil {
			route.Account = &Account{
				Id: accountId,
			}
		}
		if routerId != nil {
			route.Router = &Router{
				Id: routerId,
			}
		}
		if err := rs.refreshRouteForeigns(ctx, &route); err != nil {
			return fmt.Errorf("Can not update route foreigns: %v", err), c, l
		}
		l = append(l, &route)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of routes: %v", err), c, l
	}

	return nil, c, l
}

func (rs *PGPoolRouteStore) Delete(ctx interface{}, route *Route) (error, bool) {
	var profileId *int
	var instrumentId *int
	var accountId *int
	var routerId *int

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
		&profileId,
		&instrumentId,
		&accountId,
		&routerId,
		&route.Settings,
	)

	if profileId != nil {
		route.Profile = &Profile{
			Id: profileId,
		}
	}
	if instrumentId != nil {
		route.Instrument = &Instrument{
			Id: instrumentId,
		}
	}
	if accountId != nil {
		route.Account = &Account{
			Id: accountId,
		}
	}
	if routerId != nil {
		route.Router = &Router{
			Id: routerId,
		}
	}

	if e := rs.refreshRouteForeigns(ctx, route); e != nil {
		return fmt.Errorf("Can not update route foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func (rs *PGPoolRouteStore) Update(ctx interface{}, route *Route) (error, bool) {
	var profileId *int
	var instrumentId *int
	var accountId *int
	var routerId *int

	if route.Profile != nil {
		profileId = route.Profile.Id
	}

	if route.Instrument != nil {
		instrumentId = route.Instrument.Id
	}

	if route.Account != nil {
		accountId = route.Account.Id
	}

	if route.Router != nil {
		routerId = route.Router.Id
	}

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
		profileId,
		instrumentId,
		accountId,
		routerId,
		route.Settings,
	).Scan(
		&profileId,
		&instrumentId,
		&accountId,
		&routerId,
		&route.Settings,
	)

	if profileId != nil {
		route.Profile = &Profile{
			Id: profileId,
		}
	}
	if instrumentId != nil {
		route.Instrument = &Instrument{
			Id: instrumentId,
		}
	}
	if accountId != nil {
		route.Account = &Account{
			Id: accountId,
		}
	}
	if routerId != nil {
		route.Router = &Router{
			Id: routerId,
		}
	}

	if e := rs.refreshRouteForeigns(ctx, route); e != nil {
		return fmt.Errorf("Can not update route foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func NewPGPoolRouteStore(
	pool            *pgxpool.Pool,
	profileStore    ProfileRepository,
	instrumentStore InstrumentRepository,
	accountStore    AccountRepository,
	routerStore     RouterRepository,
	logger          LoggerFunc,
) RouteRepository {
	return &PGPoolRouteStore{
		pool:            pool,
		profileStore:    profileStore,
		instrumentStore: instrumentStore,
		accountStore:    accountStore,
		routerStore:     routerStore,
		logger:          logger,
	}
}
