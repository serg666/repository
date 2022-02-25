package repository

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type AccountSettings map[string]interface{}

type Account struct {
	Id                        *int
	IsEnabled                 *bool
	IsTest                    *bool
	RebillEnabled             *bool
	RefundEnabled             *bool
	ReversalEnabled           *bool
	PartialConfirmEnabled     *bool
	PartialReversalEnabled    *bool
	PartialRefundEnabled      *bool
	CurrencyConversionEnabled *bool
	Currency                  *Currency
	Channel                   *Channel
	Settings                  *AccountSettings
}

type AccountSpecification interface {
	ToSqlClauses() string
}

type AccountRepository interface {
	Add(ctx interface{}, account *Account) error
	Delete(ctx interface{}, account *Account) (error, bool)
	Update(ctx interface{}, account *Account) (error, bool)
	Query(ctx interface{}, specification AccountSpecification) (error, int, []*Account)
}

type AccountSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (aswlao *AccountSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", aswlao.limit, aswlao.offset)
}

type AccountSpecificationByID struct {
	id int
}

func (asbyid *AccountSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", asbyid.id)
}

func NewAccountSpecificationByID(id int) AccountSpecification {
	return &AccountSpecificationByID{id: id}
}

func NewAccountSpecificationWithLimitAndOffset(limit int, offset int) AccountSpecification {
	return &AccountSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

type PGPoolAccountStore struct {
	pool   *pgxpool.Pool
	logger LoggerFunc
}

func (as *PGPoolAccountStore) Add(ctx interface{}, account *Account) error {
	return as.pool.QueryRow(
		context.Background(),
		`insert into accounts (
			is_enabled,
			is_test,
			rebill_enabled,
			refund_enabled,
			reversal_enabled,
			partial_confirm_enabled,
			partial_reversal_enabled,
			partial_refund_enabled,
			currency_conversion_enabled,
			currency_id,
			channel_id,
			settings
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) returning id`,
		account.IsEnabled,
		account.IsTest,
		account.RebillEnabled,
		account.RefundEnabled,
		account.ReversalEnabled,
		account.PartialConfirmEnabled,
		account.PartialReversalEnabled,
		account.PartialRefundEnabled,
		account.CurrencyConversionEnabled,
		account.Currency.Id,
		account.Channel.Id,
		account.Settings,
	).Scan(&account.Id)
}

func (as *PGPoolAccountStore) Query(ctx interface{}, specification AccountSpecification) (error, int, []*Account) {
	var l []*Account
	var c int = 0

	conn, err := as.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from accounts",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get accounts cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			`select
				id,
				is_enabled,
				is_test,
				rebill_enabled,
				refund_enabled,
				reversal_enabled,
				partial_confirm_enabled,
				partial_reversal_enabled,
				partial_refund_enabled,
				currency_conversion_enabled,
				settings,
				currency_id,
				channel_id
			from accounts %s`,
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query accounts rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		account := Account{
			Currency: &Currency{},
			Channel:  &Channel{},
		}

		if err = rows.Scan(
			&account.Id,
			&account.IsEnabled,
			&account.IsTest,
			&account.RebillEnabled,
			&account.RefundEnabled,
			&account.ReversalEnabled,
			&account.PartialConfirmEnabled,
			&account.PartialReversalEnabled,
			&account.PartialRefundEnabled,
			&account.CurrencyConversionEnabled,
			&account.Settings,
			&account.Currency.Id,
			&account.Channel.Id,
		); err != nil {
			return fmt.Errorf("failed to get account row: %v", err), c, l
		}
		l = append(l, &account)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of accounts: %v", err), c, l
	}

	return nil, c, l
}

func (as *PGPoolAccountStore) Delete(ctx interface{}, account *Account) (error, bool) {
	err := as.pool.QueryRow(
		context.Background(),
		`delete from
			accounts
		where
			id=$1
		returning
			is_enabled,
			is_test,
			rebill_enabled,
			refund_enabled,
			reversal_enabled,
			partial_confirm_enabled,
			partial_reversal_enabled,
			partial_refund_enabled,
			currency_conversion_enabled,
			settings,
			currency_id,
			channel_id`,
		account.Id,
	).Scan(
		&account.IsEnabled,
		&account.IsTest,
		&account.RebillEnabled,
		&account.RefundEnabled,
		&account.ReversalEnabled,
		&account.PartialConfirmEnabled,
		&account.PartialReversalEnabled,
		&account.PartialRefundEnabled,
		&account.CurrencyConversionEnabled,
		&account.Settings,
		&account.Currency.Id,
		&account.Channel.Id,
	)

	return err, err == pgx.ErrNoRows
}

func (as *PGPoolAccountStore) Update(ctx interface{}, account *Account) (error, bool) {
	err := as.pool.QueryRow(
		context.Background(),
		`update accounts set
			is_enabled=COALESCE($2, is_enabled),
			is_test=COALESCE($3, is_test),
			rebill_enabled=COALESCE($4, rebill_enabled),
			refund_enabled=COALESCE($5, refund_enabled),
			reversal_enabled=COALESCE($6, reversal_enabled),
			partial_confirm_enabled=COALESCE($7, partial_confirm_enabled),
			partial_reversal_enabled=COALESCE($8, partial_reversal_enabled),
			partial_refund_enabled=COALESCE($9, partial_refund_enabled),
			currency_conversion_enabled=COALESCE($10, currency_conversion_enabled),
			settings=COALESCE($11, settings),
			currency_id=COALESCE($12, currency_id),
			channel_id=COALESCE($13, channel_id)
		where
			id=$1
		returning
			is_enabled,
			is_test,
			rebill_enabled,
			refund_enabled,
			reversal_enabled,
			partial_confirm_enabled,
			partial_reversal_enabled,
			partial_refund_enabled,
			currency_conversion_enabled,
			settings,
			currency_id,
			channel_id`,
		account.Id,
		account.IsEnabled,
		account.IsTest,
		account.RebillEnabled,
		account.RefundEnabled,
		account.ReversalEnabled,
		account.PartialConfirmEnabled,
		account.PartialReversalEnabled,
		account.PartialRefundEnabled,
		account.CurrencyConversionEnabled,
		account.Settings,
		account.Currency.Id,
		account.Channel.Id,
	).Scan(
		&account.IsEnabled,
		&account.IsTest,
		&account.RebillEnabled,
		&account.RefundEnabled,
		&account.ReversalEnabled,
		&account.PartialConfirmEnabled,
		&account.PartialReversalEnabled,
		&account.PartialRefundEnabled,
		&account.CurrencyConversionEnabled,
		&account.Settings,
		&account.Currency.Id,
		&account.Channel.Id,
	)

	return err, err == pgx.ErrNoRows
}

func NewPGPoolAccountStore(pool *pgxpool.Pool, logger LoggerFunc) AccountRepository {
	return &PGPoolAccountStore{
		pool:   pool,
		logger: logger,
	}
}
