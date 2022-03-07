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

func (a *Account) String() string {
	return fmt.Sprintf("Account <%d> (%s)", *a.Id, a.Channel)
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
	pool          *pgxpool.Pool
	currencyStore CurrencyRepository
	channelStore  ChannelRepository
	logger        LoggerFunc
}

func (as *PGPoolAccountStore) Add(ctx interface{}, account *Account) error {
	var currencyId *int
	var channelId *int

	if account.Currency != nil {
		currencyId = account.Currency.Id
	}

	if account.Channel != nil {
		channelId = account.Channel.Id
	}

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
		currencyId,
		channelId,
		account.Settings,
	).Scan(&account.Id)
}

func (as *PGPoolAccountStore) refreshAccountCurrency(ctx interface{}, account *Account) error {
	if !(account.Currency != nil && account.Currency.Id != nil) {
		return nil
	}

	err, _, currencies := as.currencyStore.Query(ctx, NewCurrencySpecificationByID(
		*account.Currency.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update account currency: %v", err)
	}

	for _, currency := range currencies {
		account.Currency = currency
	}

	return nil
}

func (as *PGPoolAccountStore) refreshAccountChannel(ctx interface{}, account *Account) error {
	if !(account.Channel != nil && account.Channel.Id != nil) {
		return nil
	}

	err, _, channels := as.channelStore.Query(ctx, NewChannelSpecificationByID(
		*account.Channel.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update account channel: %v", err)
	}

	for _, channel := range channels {
		account.Channel = channel
	}

	return nil
}

func (as *PGPoolAccountStore) refreshAccountForeigns(ctx interface{}, account *Account) error {
	if err := as.refreshAccountCurrency(ctx, account); err != nil {
		return err
	}

	if err := as.refreshAccountChannel(ctx, account); err != nil {
		return err
	}

	return nil
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
		var account Account
		var currencyId *int
		var channelId *int

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
			&currencyId,
			&channelId,
		); err != nil {
			return fmt.Errorf("failed to get account row: %v", err), c, l
		}
		if currencyId != nil {
			account.Currency = &Currency{
				Id: currencyId,
			}
		}
		if channelId != nil {
			account.Channel = &Channel{
				Id: channelId,
			}
		}
		if err := as.refreshAccountForeigns(ctx, &account); err != nil {
			return fmt.Errorf("Can not update account foreigns: %v", err), c, l
		}
		l = append(l, &account)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of accounts: %v", err), c, l
	}

	return nil, c, l
}

func (as *PGPoolAccountStore) Delete(ctx interface{}, account *Account) (error, bool) {
	var currencyId *int
	var channelId *int

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
		&currencyId,
		&channelId,
	)

	if currencyId != nil {
		account.Currency = &Currency{
			Id: currencyId,
		}
	}
	if channelId != nil {
		account.Channel = &Channel{
			Id: channelId,
		}
	}

	if e := as.refreshAccountForeigns(ctx, account); e != nil {
		return fmt.Errorf("Can not update account foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func (as *PGPoolAccountStore) Update(ctx interface{}, account *Account) (error, bool) {
	var currencyId *int
	var channelId *int

	if account.Currency != nil {
		currencyId = account.Currency.Id
	}

	if account.Channel != nil {
		channelId = account.Channel.Id
	}

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
		currencyId,
		channelId,
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
		&currencyId,
		&channelId,
	)

	if currencyId != nil {
		account.Currency = &Currency{
			Id: currencyId,
		}
	}
	if channelId != nil {
		account.Channel = &Channel{
			Id: channelId,
		}
	}

	if e := as.refreshAccountForeigns(ctx, account); e != nil {
		return fmt.Errorf("Can not update account foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func NewPGPoolAccountStore(
	pool *pgxpool.Pool,
	currencyStore CurrencyRepository,
	channelStore ChannelRepository,
	logger LoggerFunc,
) AccountRepository {
	return &PGPoolAccountStore{
		pool:          pool,
		currencyStore: currencyStore,
		channelStore:  channelStore,
		logger:        logger,
	}
}
