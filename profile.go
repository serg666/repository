package repository

import (
	"fmt"
	"sync"
	"context"
	"github.com/wk8/go-ordered-map"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Profile struct {
	Id          *int      `json:"id"`
	Key         *string   `json:"key"`
	Description *string   `json:"description"`
	Currency    *Currency `json:"currency"`
}

type ProfileSpecification interface {
	Specified(profile *Profile, i int) bool
	ToSqlClauses() string
}

type ProfileRepository interface {
	Add(ctx interface{}, profile *Profile) error
	Delete(ctx interface{}, profile *Profile) (error, bool)
	Update(ctx interface{}, profile *Profile) (error, bool)
	Query(ctx interface{}, specification ProfileSpecification) (error, int, []*Profile)
}

type ProfileSpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (pswlao *ProfileSpecificationWithLimitAndOffset) Specified(profile *Profile, i int) bool {
	return i >= pswlao.offset && i < pswlao.offset + pswlao.limit
}

func (pswlao *ProfileSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", pswlao.limit, pswlao.offset)
}

type ProfileSpecificationByID struct {
	id int
}

func (psbyid *ProfileSpecificationByID) Specified(profile *Profile, i int) bool {
	return psbyid.id == *profile.Id
}

func (psbyid *ProfileSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", psbyid.id)
}

type ProfileSpecificationByKey struct {
	key string
}

func (psbykey *ProfileSpecificationByKey) Specified(profile *Profile, i int) bool {
	return psbykey.key == *profile.Key
}

func (psbykey *ProfileSpecificationByKey) ToSqlClauses() string {
	return fmt.Sprintf("where key='%s'", psbykey.key)
}

type OrderedMapProfileStore struct {
	sync.Mutex

	profiles      *orderedmap.OrderedMap
	nextId        int
	currencyStore CurrencyRepository
	logger        LoggerFunc
}

func (ps *OrderedMapProfileStore) Add(ctx interface{}, profile *Profile) error {
	ps.Lock()
	defer ps.Unlock()

	id := ps.nextId
	profile.Id = &id
	ps.profiles.Set(*profile.Id, *profile)
	ps.nextId++

	return nil
}

func (ps *OrderedMapProfileStore) refreshProfileCurrency(ctx interface{}, profile *Profile) error {
	if !(profile.Currency != nil && profile.Currency.Id != nil) {
		return nil
	}

	err, _, currencies := ps.currencyStore.Query(ctx, NewCurrencySpecificationByID(
		*profile.Currency.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update profile currency: %v", err)
	}

	for _, currency := range currencies {
		profile.Currency = currency
	}

	return nil
}

func (ps *OrderedMapProfileStore) refreshProfileForeigns(ctx interface{}, profile *Profile) error {
	if err := ps.refreshProfileCurrency(ctx, profile); err != nil {
		return err
	}

	return nil
}

func (ps *OrderedMapProfileStore) Delete(ctx interface{}, profile *Profile) (error, bool) {
	ps.Lock()
	defer ps.Unlock()

	value, present := ps.profiles.Delete(*profile.Id)
	if !present {
		return fmt.Errorf("profile with id=%v not found", *profile.Id), true
	}

	deleted := value.(Profile)
	profile.Key = deleted.Key
	profile.Description = deleted.Description
	profile.Currency = deleted.Currency

	if err := ps.refreshProfileForeigns(ctx, profile); err != nil {
		return fmt.Errorf("Can not update profile foreigns: %v", err), false
	}

	return nil, false
}

func (ps *OrderedMapProfileStore) Update(ctx interface{}, profile *Profile) (error, bool) {
	ps.Lock()
	defer ps.Unlock()

	value, present := ps.profiles.Get(*profile.Id)
	if !present {
		return fmt.Errorf("profile with id=%v not found", *profile.Id), true
	}

	old := value.(Profile)

	if profile.Key != nil {
		old.Key = profile.Key
	} else {
		profile.Key = old.Key
	}

	if profile.Description != nil {
		old.Description = profile.Description
	} else {
		profile.Description = old.Description
	}

	if profile.Currency != nil {
		old.Currency = profile.Currency
	} else {
		profile.Currency = old.Currency
	}

	ps.profiles.Set(*old.Id, old)

	if err := ps.refreshProfileForeigns(ctx, profile); err != nil {
		return fmt.Errorf("Can not update profile foreigns: %v", err), false
	}

	return nil, false
}

func (ps *OrderedMapProfileStore) Query(ctx interface{}, specification ProfileSpecification) (error, int, []*Profile) {
	ps.Lock()
	defer ps.Unlock()

	var l []*Profile
	var c int = 0

	for el := ps.profiles.Oldest(); el != nil; el = el.Next() {
		profile := el.Value.(Profile)
		if specification.Specified(&profile, c) {
			if err := ps.refreshProfileForeigns(ctx, &profile); err != nil {
				return fmt.Errorf("Can not update profile foreigns: %v", err), c, l
			}
			l = append(l, &profile)
		}
		c++
	}

	return nil, ps.profiles.Len(), l
}

func NewOrderedMapProfileStore(
	profiles *orderedmap.OrderedMap,
	currencyStore CurrencyRepository,
	logger LoggerFunc,
) ProfileRepository {
	return &OrderedMapProfileStore{
		profiles:      profiles,
		nextId:        1,
		currencyStore: currencyStore,
		logger:        logger,
	}
}

func NewProfileSpecificationByID(id int) ProfileSpecification {
	return &ProfileSpecificationByID{
		id: id,
	}
}

func NewProfileSpecificationByKey(key string) ProfileSpecification {
	return &ProfileSpecificationByKey{
		key: key,
	}
}

func NewProfileSpecificationWithLimitAndOffset(limit int, offset int) ProfileSpecification {
	return &ProfileSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

type PGPoolProfileStore struct {
	pool          *pgxpool.Pool
	currencyStore CurrencyRepository
	logger        LoggerFunc
}

func (ps *PGPoolProfileStore) Add(ctx interface{}, profile *Profile) error {
	var currencyId *int

	if profile.Currency != nil {
		currencyId = profile.Currency.Id
	}

	return ps.pool.QueryRow(
		context.Background(),
		`insert into profiles (
			key,
			description,
			currency_id
		) values ($1, $2, $3) returning id`,
		profile.Key,
		profile.Description,
		currencyId,
	).Scan(&profile.Id)
}

func (ps *PGPoolProfileStore) refreshProfileCurrency(ctx interface{}, profile *Profile) error {
	if !(profile.Currency != nil && profile.Currency.Id != nil) {
		return nil
	}

	err, _, currencies := ps.currencyStore.Query(ctx, NewCurrencySpecificationByID(
		*profile.Currency.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update profile currency: %v", err)
	}

	for _, currency := range currencies {
		profile.Currency = currency
	}

	return nil
}

func (ps *PGPoolProfileStore) refreshProfileForeigns(ctx interface{}, profile *Profile) error {
	if err := ps.refreshProfileCurrency(ctx, profile); err != nil {
		return err
	}

	return nil
}

func (ps *PGPoolProfileStore) Delete(ctx interface{}, profile *Profile) (error, bool) {
	var currencyId *int

	err := ps.pool.QueryRow(
		context.Background(),
		`delete from
			profiles
		where
			id=$1
		returning
			key,
			description,
			currency_id`,
		profile.Id,
	).Scan(
		&profile.Key,
		&profile.Description,
		&currencyId,
	)

	if currencyId != nil {
		profile.Currency = &Currency{
			Id: currencyId,
		}
	}

	if e := ps.refreshProfileForeigns(ctx, profile); e != nil {
		return fmt.Errorf("Can not update profile foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func (ps *PGPoolProfileStore) Query(ctx interface{}, specification ProfileSpecification) (error, int, []*Profile) {
	var l []*Profile
	var c int = 0

	conn, err := ps.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from profiles",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get profiles cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			`select
				id,
				key,
				description,
				currency_id
			from profiles %s`,
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query profiles rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var profile Profile
		var currencyId *int

		if err = rows.Scan(
			&profile.Id,
			&profile.Key,
			&profile.Description,
			&currencyId,
		); err != nil {
			return fmt.Errorf("failed to get profile row: %v", err), c, l
		}
		if currencyId != nil {
			profile.Currency = &Currency{
				Id: currencyId,
			}
		}
		if err := ps.refreshProfileForeigns(ctx, &profile); err != nil {
			return fmt.Errorf("Can not update profile foreigns: %v", err), c, l
		}
		l = append(l, &profile)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of profiles: %v", err), c, l
	}

	return nil, c, l
}

func (ps *PGPoolProfileStore) Update(ctx interface{}, profile *Profile) (error, bool) {
	var currencyId *int

	if profile.Currency != nil {
		currencyId = profile.Currency.Id
	}

	err := ps.pool.QueryRow(
		context.Background(),
		`update profiles set
			key=COALESCE($2, key),
			description=COALESCE($3, description),
			currency_id=COALESCE($4, currency_id)
		where
			id=$1
		returning
			key,
			description,
			currency_id`,
		profile.Id,
		profile.Key,
		profile.Description,
		currencyId,
	).Scan(
		&profile.Key,
		&profile.Description,
		&currencyId,
	)

	if currencyId != nil {
		profile.Currency = &Currency{
			Id: currencyId,
		}
	}

	if e := ps.refreshProfileForeigns(ctx, profile); e != nil {
		return fmt.Errorf("Can not update profile foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func NewPGPoolProfileStore(
	pool          *pgxpool.Pool,
	currencyStore CurrencyRepository,
	logger        LoggerFunc,
) ProfileRepository {
	return &PGPoolProfileStore{
		pool:          pool,
		currencyStore: currencyStore,
		logger:        logger,
	}
}
