package repository

import (
	"fmt"
	"sync"
	"context"
	"github.com/wk8/go-ordered-map"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Currency struct {
	Id          *int    `json:"id"`
	NumericCode *int    `json:"numeric_code"`
	Name        *string `json:"name"`
	CharCode    *string `json:"char_code"`
	Exponent    *int    `json:"exponent"`
}

type CurrencySpecification interface {
	Specified(currency *Currency, i int) bool
	ToSqlClauses() string
}

type CurrencyRepository interface {
	Add(ctx interface{}, currency *Currency) error
	Delete(ctx interface{}, currency *Currency) (error, bool)
	Update(ctx interface{}, currency *Currency) (error, bool)
	Query(ctx interface{}, specification CurrencySpecification) (error, int, []*Currency)
}

type CurrencySpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (cswlao *CurrencySpecificationWithLimitAndOffset) Specified(currency *Currency, i int) bool {
	return i >= cswlao.offset && i < cswlao.offset + cswlao.limit
}

func (cswlao *CurrencySpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", cswlao.limit, cswlao.offset)
}

type CurrencySpecificationByID struct {
	id int
}

func (csbyid *CurrencySpecificationByID) Specified(currency *Currency, i int) bool {
	return csbyid.id == *currency.Id
}

func (csbyid *CurrencySpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", csbyid.id)
}

type CurrencySpecificationByNumericCode struct {
	numericcode int
}

func (csbync *CurrencySpecificationByNumericCode) Specified(currency *Currency, i int) bool {
	return csbync.numericcode == *currency.NumericCode
}

func (csbync *CurrencySpecificationByNumericCode) ToSqlClauses() string {
	return fmt.Sprintf("where numeric_code=%d", csbync.numericcode)
}

type OrderedMapCurrencyStore struct {
	sync.Mutex

	currencies *orderedmap.OrderedMap
	nextId     int
	logger     LoggerFunc
}

func (cs *OrderedMapCurrencyStore) Add(ctx interface{}, currency *Currency) error {
	cs.Lock()
	defer cs.Unlock()

	id := cs.nextId
	currency.Id = &id
	cs.currencies.Set(*currency.Id, *currency)
	cs.nextId++

	return nil
}

func (cs *OrderedMapCurrencyStore) Delete(ctx interface{}, currency *Currency) (error, bool) {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.currencies.Delete(*currency.Id)
	if !present {
		return fmt.Errorf("currency with id=%v not found", *currency.Id), true
	}

	deleted := value.(Currency)
	currency.NumericCode = deleted.NumericCode
	currency.Name = deleted.Name
	currency.CharCode = deleted.CharCode
	currency.Exponent = deleted.Exponent

	return nil, false
}

func (cs *OrderedMapCurrencyStore) Update(ctx interface{}, currency *Currency) (error, bool) {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.currencies.Get(*currency.Id)
	if !present {
		return fmt.Errorf("currency with id=%v not found", *currency.Id), true
	}

	old := value.(Currency)

	if currency.NumericCode != nil {
		old.NumericCode = currency.NumericCode
	} else {
		currency.NumericCode = old.NumericCode
	}

	if currency.Name != nil {
		old.Name = currency.Name
	} else {
		currency.Name = old.Name
	}

	if currency.CharCode != nil {
		old.CharCode = currency.CharCode
	} else {
		currency.CharCode = old.CharCode
	}

	if currency.Exponent != nil {
		old.Exponent = currency.Exponent
	} else {
		currency.Exponent = old.Exponent
	}

	cs.currencies.Set(*old.Id, old)

	return nil, false
}

func (cs *OrderedMapCurrencyStore) Query(ctx interface{}, specification CurrencySpecification) (error, int, []*Currency) {
	cs.Lock()
	defer cs.Unlock()

	var l []*Currency
	var c int = 0

	for el := cs.currencies.Oldest(); el != nil; el = el.Next() {
		currency := el.Value.(Currency)
		if specification.Specified(&currency, c) {
			l = append(l, &currency)
		}
		c++
	}

	return nil, cs.currencies.Len(), l
}

func NewOrderedMapCurrencyStore(currencies *orderedmap.OrderedMap, logger LoggerFunc) CurrencyRepository {
	return &OrderedMapCurrencyStore{
		currencies: currencies,
		nextId:     1,
		logger:     logger,
	}
}

func NewCurrencySpecificationByID(id int) CurrencySpecification {
	return &CurrencySpecificationByID{id: id}
}

func NewCurrencySpecificationByNumericCode(numericcode int) CurrencySpecification {
	return &CurrencySpecificationByNumericCode{
		numericcode: numericcode,
	}
}

func NewCurrencySpecificationWithLimitAndOffset(limit int, offset int) CurrencySpecification {
	return &CurrencySpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

type PGPoolCurrencyStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (cs *PGPoolCurrencyStore) Add(ctx interface{}, currency *Currency) error {
	return cs.pool.QueryRow(
		context.Background(),
		"insert into currencies (numeric_code, name, char_code, exponent) values ($1, $2, $3, $4) returning id",
		currency.NumericCode,
		currency.Name,
		currency.CharCode,
		currency.Exponent,
	).Scan(&currency.Id)
}

func (cs *PGPoolCurrencyStore) Delete(ctx interface{}, currency *Currency) (error, bool) {
	err := cs.pool.QueryRow(
		context.Background(),
		"delete from currencies where id=$1 returning numeric_code, name, char_code, exponent",
		currency.Id,
	).Scan(
		&currency.NumericCode,
		&currency.Name,
		&currency.CharCode,
		&currency.Exponent,
	)

	return err, err == pgx.ErrNoRows
}

func (cs *PGPoolCurrencyStore) Query(ctx interface{}, specification CurrencySpecification) (error, int, []*Currency) {
	var l []*Currency
	var c int = 0

	conn, err := cs.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from currencies",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get currencies cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			"select id, numeric_code, name, char_code, exponent from currencies %s",
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query currencies rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var currency Currency

		if err = rows.Scan(
			&currency.Id,
			&currency.NumericCode,
			&currency.Name,
			&currency.CharCode,
			&currency.Exponent,
		); err != nil {
			return fmt.Errorf("failed to get currency row: %v", err), c, l
		}
		l = append(l, &currency)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of currencies: %v", err), c, l
	}

	return nil, c, l
}

func (cs *PGPoolCurrencyStore) Update(ctx interface{}, currency *Currency) (error, bool) {
	err := cs.pool.QueryRow(
		context.Background(),
		`update currencies set
			numeric_code=COALESCE($2, numeric_code),
			name=COALESCE($3, name),
			char_code=COALESCE($4, char_code),
			exponent=COALESCE($5, exponent)
		where id=$1 returning numeric_code, name, char_code, exponent`,
		currency.Id,
		currency.NumericCode,
		currency.Name,
		currency.CharCode,
		currency.Exponent,
	).Scan(
		&currency.NumericCode,
		&currency.Name,
		&currency.CharCode,
		&currency.Exponent,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolCurrencyStore(pool *pgxpool.Pool, logger LoggerFunc) CurrencyRepository {
	return &PGPoolCurrencyStore{
		pool:   pool,
		logger: logger,
	}
}
