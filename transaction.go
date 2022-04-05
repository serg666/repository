package repository

import (
	"fmt"
	"time"
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type AdditionalData map[string]interface{}

type ThreeDSecure10 struct {
	AcsUrl *string
	PaReq  *string
}

type ThreeDSecure20 struct {
	AcsUrl             *string
	Creq               *string
}

type ThreeDSMethodUrl struct {
	MethodUrl         *string
	ThreeDSMethodData *string
}

type Transaction struct {
	Id                *int
	Created           *time.Time
	Type              *string
	Status            *string
	Profile           *Profile
	Account           *Account
	Instrument        *Instrument
	InstrumentId      *int
	Amount            *uint
	Currency          *Currency
	AmountConverted   *uint
	CurrencyConverted *Currency
	AuthCode          *string
	RRN               *string
	ResponseCode      *string
	ErrorMessage      *string
	RemoteId          *string
	OrderId           *string
	Reference         *Transaction
	ThreeDSecure10    *ThreeDSecure10
	ThreeDSecure20    *ThreeDSecure20
	ThreeDSMethodUrl  *ThreeDSMethodUrl
	AdditionalData    *AdditionalData
	Customer          *string
}

func (tx *Transaction) New() {
	txStatus := NEW
	tx.Status = &txStatus
}

func (tx *Transaction) Success() {
	txStatus := SUCCESS
	tx.Status = &txStatus
}

func (tx *Transaction) IsSuccess() bool {
	return *tx.Status == SUCCESS
}

func (tx *Transaction) Declined(errorMsg *string) {
	txStatus := DECLINED
	tx.Status = &txStatus
	tx.ErrorMessage = errorMsg
}

func (tx *Transaction) Wait3DS() {
	txStatus := WAIT3DS
	tx.Status = &txStatus
}

func (tx *Transaction) Is3DSWaiting() bool {
	return *tx.Status == WAIT3DS
}

func (tx *Transaction) WaitMethodUrl() {
	txStatus := WAITMETHODURL
	tx.Status = &txStatus
}

func (tx *Transaction) IsMethodUrlWaiting() bool {
	return *tx.Status == WAITMETHODURL
}

func (tx *Transaction) InFinalState() bool {
	switch *tx.Status {
	case
		SUCCESS,
		DECLINED:
		return true
	}
	return false
}

func (tx *Transaction) IsPreAuth() bool {
	return *tx.Type == PREAUTH
}

func (tx *Transaction) IsAuth() bool {
	return *tx.Type == AUTH
}

func NewTransaction(
	txType string,
	orderId *string,
	profile *Profile,
	account *Account,
	instrument *Instrument,
	instrumentId *int,
	amount *uint,
	customer *string,
	reference *Transaction,
) *Transaction {
	transaction := &Transaction{
		Type: &txType,
		Profile: profile,
		Account: account,
		Instrument: instrument,
		InstrumentId: instrumentId,
		Currency: profile.Currency,
		Amount: amount,
		AmountConverted: amount, // @todo: convert amount to account currency from profile currency
		CurrencyConverted: account.Currency,
		OrderId: orderId,
		Reference: reference,
		Customer: customer,
	}

	transaction.New()

	return transaction
}

type TurnOverResult struct {
	Cnt uint
	Sum uint
}

type TransactionSpecification interface {
	ToSqlClauses() string
}

type TransactionRepository interface {
	Add(ctx interface{}, transaction *Transaction) error
	Update(ctx interface{}, transaction *Transaction) (error, bool)
	Query(ctx interface{}, specification TransactionSpecification) (error, int, []*Transaction)
	TypeTurnOver(ctx interface{}, specification TransactionSpecification) (error, *map[string]TurnOverResult)
}

type TransactionSpecificationWithLimitAndOffset struct {
	limit  int
	offset int
}

func (tswlao *TransactionSpecificationWithLimitAndOffset) ToSqlClauses() string {
	return fmt.Sprintf("limit %d offset %d", tswlao.limit, tswlao.offset)
}

type TransactionSpecificationByID struct {
	id int
}

func (tsbyid *TransactionSpecificationByID) ToSqlClauses() string {
	return fmt.Sprintf("where id=%d", tsbyid.id)
}

type TransactionSpecificationByReferenceIdAndStatus struct {
	id     int
	status string
}

func (spec *TransactionSpecificationByReferenceIdAndStatus) ToSqlClauses() string {
	return fmt.Sprintf("where reference_id=%d and status='%s'", spec.id, spec.status)
}

func NewTransactionSpecificationByID(id int) TransactionSpecification {
	return &TransactionSpecificationByID{id: id}
}

func NewTransactionSpecificationWithLimitAndOffset(limit int, offset int) TransactionSpecification {
	return &TransactionSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

func NewTransactionSpecificationByReferenceIdAndStatus(id int, status string) TransactionSpecification {
	return &TransactionSpecificationByReferenceIdAndStatus{
		id:     id,
		status: status,
	}
}

type PGPoolTransactionStore struct {
	pool            *pgxpool.Pool
	profileStore    ProfileRepository
	instrumentStore InstrumentRepository
	accountStore    AccountRepository
	currencyStore   CurrencyRepository
	logger          LoggerFunc
}

func (ts *PGPoolTransactionStore) Add(ctx interface{}, transaction *Transaction) error {
	var profileId           *int
	var accountId           *int
	var instrumentId        *int
	var currencyId          *int
	var currencyConvertedId *int
	var referenceId         *int

	if transaction.Profile != nil {
		profileId = transaction.Profile.Id
	}

	if transaction.Account != nil {
		accountId = transaction.Account.Id
	}

	if transaction.Instrument != nil {
		instrumentId = transaction.Instrument.Id
	}

	if transaction.Currency != nil {
		currencyId = transaction.Currency.Id
	}

	if transaction.CurrencyConverted != nil {
		currencyConvertedId = transaction.CurrencyConverted.Id
	}

	if transaction.Reference != nil {
		referenceId = transaction.Reference.Id
	}

	return ts.pool.QueryRow(
		context.Background(),
		`insert into transactions (
			type,
			status,
			profile_id,
			account_id,
			instrument_id,
			instrument,
			amount,
			currency_id,
			amount_converted,
			currency_converted_id,
			authcode,
			rrn,
			response_code,
			remote_id,
			order_id,
			reference_id,
			threedsecure10,
			threedsecure20,
			threedsmethodurl,
			error_message,
			additional_data,
			customer
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22) returning id, created`,
		transaction.Type,
		transaction.Status,
		profileId,
		accountId,
		instrumentId,
		transaction.InstrumentId,
		transaction.Amount,
		currencyId,
		transaction.AmountConverted,
		currencyConvertedId,
		transaction.AuthCode,
		transaction.RRN,
		transaction.ResponseCode,
		transaction.RemoteId,
		transaction.OrderId,
		referenceId,
		transaction.ThreeDSecure10,
		transaction.ThreeDSecure20,
		transaction.ThreeDSMethodUrl,
		transaction.ErrorMessage,
		transaction.AdditionalData,
		transaction.Customer,
	).Scan(&transaction.Id, &transaction.Created)
}

func (ts *PGPoolTransactionStore) refreshTransactionProfile(ctx interface{}, transaction *Transaction) error {
	if !(transaction.Profile != nil && transaction.Profile.Id != nil) {
		return nil
	}

	err, _, profiles := ts.profileStore.Query(ctx, NewProfileSpecificationByID(
		*transaction.Profile.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction profile: %v", err)
	}

	for _, profile := range profiles {
		transaction.Profile = profile
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionAccount(ctx interface{}, transaction *Transaction) error {
	if !(transaction.Account != nil && transaction.Account.Id != nil) {
		return nil
	}

	err, _, accounts := ts.accountStore.Query(ctx, NewAccountSpecificationByID(
		*transaction.Account.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction account: %v", err)
	}

	for _, account := range accounts {
		transaction.Account = account
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionInstrument(ctx interface{}, transaction *Transaction) error {
	if !(transaction.Instrument != nil && transaction.Instrument.Id != nil) {
		return nil
	}

	err, _, instruments := ts.instrumentStore.Query(ctx, NewInstrumentSpecificationByID(
		*transaction.Instrument.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction instrument: %v", err)
	}

	for _, instrument := range instruments {
		transaction.Instrument = instrument
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionCurrency(ctx interface{}, transaction *Transaction) error {
	if !(transaction.Currency != nil && transaction.Currency.Id != nil) {
		return nil
	}

	err, _, currencies := ts.currencyStore.Query(ctx, NewCurrencySpecificationByID(
		*transaction.Currency.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction currency: %v", err)
	}

	for _, currency := range currencies {
		transaction.Currency = currency
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionCurrencyConverted(ctx interface{}, transaction *Transaction) error {
	if !(transaction.CurrencyConverted != nil && transaction.CurrencyConverted.Id != nil) {
		return nil
	}

	err, _, currencies := ts.currencyStore.Query(ctx, NewCurrencySpecificationByID(
		*transaction.CurrencyConverted.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction currency converted: %v", err)
	}

	for _, currency := range currencies {
		transaction.CurrencyConverted = currency
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionReference(ctx interface{}, transaction *Transaction) error {
	if !(transaction.Reference != nil && transaction.Reference.Id != nil) {
		return nil
	}

	err, _, transactions := ts.Query(ctx, NewTransactionSpecificationByID(
		*transaction.Reference.Id,
	))

	if err != nil {
		return fmt.Errorf("Can not update transaction reference: %v", err)
	}

	for _, tx := range transactions {
		transaction.Reference = tx
	}

	return nil
}

func (ts *PGPoolTransactionStore) refreshTransactionForeigns(ctx interface{}, transaction *Transaction) error {
	if err := ts.refreshTransactionProfile(ctx, transaction); err != nil {
		return err
	}

	if err := ts.refreshTransactionAccount(ctx, transaction); err != nil {
		return err
	}

	if err := ts.refreshTransactionInstrument(ctx, transaction); err != nil {
		return err
	}

	if err := ts.refreshTransactionCurrency(ctx, transaction); err != nil {
		return err
	}

	if err := ts.refreshTransactionCurrencyConverted(ctx, transaction); err != nil {
		return err
	}

	if err := ts.refreshTransactionReference(ctx, transaction); err != nil {
		return err
	}

	return nil
}

func (ts *PGPoolTransactionStore) TypeTurnOver(ctx interface{}, specification TransactionSpecification) (error, *map[string]TurnOverResult) {
	result := make(map[string]TurnOverResult)

	rows, err := ts.pool.Query(
		context.Background(), fmt.Sprintf(
			`select
				type,
				count(id),
				sum(amount)
			from transactions %s group by type`,
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query type turn over rows: %v", err), &result
	}
	defer rows.Close()

	for rows.Next() {
		var opType string
		var turnOverResult TurnOverResult

		if err := rows.Scan(&opType, &turnOverResult.Cnt, &turnOverResult.Sum); err != nil {
			return fmt.Errorf("failed to get type turn over row: %v", err), &result
		}

		result[opType] = turnOverResult
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of type turn over: %v", err), &result
	}

	return nil, &result
}

func (ts *PGPoolTransactionStore) Query(ctx interface{}, specification TransactionSpecification) (error, int, []*Transaction) {
	var l []*Transaction
	var c int = 0

	conn, err := ts.pool.Acquire(context.Background())

	if err != nil {
		return fmt.Errorf("failed to acquire connection from the pool: %v", err), c, l
	}
	defer conn.Release()

	err = conn.QueryRow(
		context.Background(),
		"select count(*) from transactions",
	).Scan(&c)

	if err != nil {
		return fmt.Errorf("failed to get transactions cnt: %v", err), c, l
	}

	rows, err := conn.Query(
		context.Background(), fmt.Sprintf(
			`select
				id,
				created,
				type,
				status,
				profile_id,
				account_id,
				instrument_id,
				instrument,
				amount,
				currency_id,
				amount_converted,
				currency_converted_id,
				authcode,
				rrn,
				response_code,
				remote_id,
				order_id,
				reference_id,
				threedsecure10,
				threedsecure20,
				threedsmethodurl,
				error_message,
				additional_data,
				customer
			from transactions %s`,
			specification.ToSqlClauses(),
		),
	)

	if err != nil {
		return fmt.Errorf("failed to query transactions rows: %v", err), c, l
	}
	defer rows.Close()

	for rows.Next() {
		var transaction Transaction
		var profileId *int
		var accountId *int
		var instrumentId *int
		var currencyId *int
		var currencyConvertedId *int
		var referenceId *int

		if err = rows.Scan(
			&transaction.Id,
			&transaction.Created,
			&transaction.Type,
			&transaction.Status,
			&profileId,
			&accountId,
			&instrumentId,
			&transaction.InstrumentId,
			&transaction.Amount,
			&currencyId,
			&transaction.AmountConverted,
			&currencyConvertedId,
			&transaction.AuthCode,
			&transaction.RRN,
			&transaction.ResponseCode,
			&transaction.RemoteId,
			&transaction.OrderId,
			&referenceId,
			&transaction.ThreeDSecure10,
			&transaction.ThreeDSecure20,
			&transaction.ThreeDSMethodUrl,
			&transaction.ErrorMessage,
			&transaction.AdditionalData,
			&transaction.Customer,
		); err != nil {
			return fmt.Errorf("failed to get transaction row: %v", err), c, l
		}
		if profileId != nil {
			transaction.Profile = &Profile{
				Id: profileId,
			}
		}
		if accountId != nil {
			transaction.Account = &Account{
				Id: accountId,
			}
		}
		if instrumentId != nil {
			transaction.Instrument = &Instrument{
				Id: instrumentId,
			}
		}
		if currencyId != nil {
			transaction.Currency = &Currency{
				Id: currencyId,
			}
		}
		if currencyConvertedId != nil {
			transaction.CurrencyConverted = &Currency{
				Id: currencyConvertedId,
			}
		}
		if referenceId != nil {
			transaction.Reference = &Transaction{
				Id: referenceId,
			}
		}
		if err := ts.refreshTransactionForeigns(ctx, &transaction); err != nil {
			return fmt.Errorf("Can not update transaction foreigns: %v", err), c, l
		}
		l = append(l, &transaction)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed to iterating over rows of transactions: %v", err), c, l
	}

	return nil, c, l
}

func (ts *PGPoolTransactionStore) Update(ctx interface{}, transaction *Transaction) (error, bool) {
	var profileId *int
	var accountId *int
	var instrumentId *int
	var currencyId *int
	var currencyConvertedId *int
	var referenceId *int

	if transaction.Profile != nil {
		profileId = transaction.Profile.Id
	}

	if transaction.Account != nil {
		accountId = transaction.Account.Id
	}

	if transaction.Instrument != nil {
		instrumentId = transaction.Instrument.Id
	}

	if transaction.Currency != nil {
		currencyId = transaction.Currency.Id
	}

	if transaction.CurrencyConverted != nil {
		currencyConvertedId = transaction.CurrencyConverted.Id
	}

	if transaction.Reference != nil {
		referenceId = transaction.Reference.Id
	}

	err := ts.pool.QueryRow(
		context.Background(),
		`update transactions set
			type=COALESCE($2, type),
			status=COALESCE($3, status),
			profile_id=COALESCE($4, profile_id),
			account_id=COALESCE($5, account_id),
			instrument_id=COALESCE($6, instrument_id),
			instrument=COALESCE($7, instrument),
			amount=COALESCE($8, amount),
			currency_id=COALESCE($9, currency_id),
			amount_converted=COALESCE($10, amount_converted),
			currency_converted_id=COALESCE($11, currency_converted_id),
			authcode=COALESCE($12, authcode),
			rrn=COALESCE($13, rrn),
			response_code=COALESCE($14, response_code),
			remote_id=COALESCE($15, remote_id),
			order_id=COALESCE($16, order_id),
			reference_id=COALESCE($17, reference_id),
			threedsecure10=COALESCE($18, threedsecure10),
			threedsecure20=COALESCE($19, threedsecure20),
			threedsmethodurl=COALESCE($20, threedsmethodurl),
			error_message=COALESCE($21, error_message),
			additional_data=COALESCE($22, additional_data),
			customer=COALESCE($23, customer)
		where
			id=$1
		returning
			type,
			status,
			profile_id,
			account_id,
			instrument_id,
			instrument,
			amount,
			currency_id,
			amount_converted,
			currency_converted_id,
			authcode,
			rrn,
			response_code,
			remote_id,
			order_id,
			reference_id,
			threedsecure10,
			threedsecure20,
			threedsmethodurl,
			error_message,
			additional_data,
			customer`,
		transaction.Id,
		transaction.Type,
		transaction.Status,
		profileId,
		accountId,
		instrumentId,
		transaction.InstrumentId,
		transaction.Amount,
		currencyId,
		transaction.AmountConverted,
		currencyConvertedId,
		transaction.AuthCode,
		transaction.RRN,
		transaction.ResponseCode,
		transaction.RemoteId,
		transaction.OrderId,
		referenceId,
		transaction.ThreeDSecure10,
		transaction.ThreeDSecure20,
		transaction.ThreeDSMethodUrl,
		transaction.ErrorMessage,
		transaction.AdditionalData,
		transaction.Customer,
	).Scan(
		&transaction.Type,
		&transaction.Status,
		&profileId,
		&accountId,
		&instrumentId,
		&transaction.InstrumentId,
		&transaction.Amount,
		&currencyId,
		&transaction.AmountConverted,
		&currencyConvertedId,
		&transaction.AuthCode,
		&transaction.RRN,
		&transaction.ResponseCode,
		&transaction.RemoteId,
		&transaction.OrderId,
		&referenceId,
		&transaction.ThreeDSecure10,
		&transaction.ThreeDSecure20,
		&transaction.ThreeDSMethodUrl,
		&transaction.ErrorMessage,
		&transaction.AdditionalData,
		&transaction.Customer,
	)

	if profileId != nil {
		transaction.Profile = &Profile{
			Id: profileId,
		}
	}
	if accountId != nil {
		transaction.Account = &Account{
			Id: accountId,
		}
	}
	if instrumentId != nil {
		transaction.Instrument = &Instrument{
			Id: instrumentId,
		}
	}
	if currencyId != nil {
		transaction.Currency = &Currency{
			Id: currencyId,
		}
	}
	if currencyConvertedId != nil {
		transaction.CurrencyConverted = &Currency{
			Id: currencyConvertedId,
		}
	}
	if referenceId != nil {
		transaction.Reference = &Transaction{
			Id: referenceId,
		}
	}

	if e := ts.refreshTransactionForeigns(ctx, transaction); e != nil {
		return fmt.Errorf("Can not update transaction foreigns: %v", e), err == pgx.ErrNoRows
	}

	return err, err == pgx.ErrNoRows
}

func NewPGPoolTransactionStore(
	pool            *pgxpool.Pool,
	profileStore    ProfileRepository,
	instrumentStore InstrumentRepository,
	accountStore    AccountRepository,
	currencyStore   CurrencyRepository,
	logger          LoggerFunc,
) TransactionRepository {
	return &PGPoolTransactionStore{
		pool:            pool,
		profileStore:    profileStore,
		instrumentStore: instrumentStore,
		accountStore:    accountStore,
		currencyStore:   currencyStore,
		logger:          logger,
	}
}
