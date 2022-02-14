package repository

import (
	"fmt"
	"sync"
	"github.com/wk8/go-ordered-map"
)

type Currency struct {
	Id          int
	NumericCode *int
	Name        *string
	CharCode    *string
	Exponent    *int
}

type CurrencySpecification interface {
	Specified(currency *Currency, i int) bool
}

type CurrencyRepository interface {
	Add(currency *Currency) error
	Delete(currency *Currency) error
	Update(currency *Currency) error
	Query(specification CurrencySpecification) (int, []Currency)
}

type CurrencySpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (cswlao *CurrencySpecificationWithLimitAndOffset) Specified(currency *Currency, i int) bool {
	return i >= cswlao.offset && i < cswlao.offset + cswlao.limit
}

type CurrencySpecificationByID struct {
	id int
}

func (csbyid *CurrencySpecificationByID) Specified(currency *Currency, i int) bool {
	return csbyid.id == currency.Id
}

type CurrencySpecificationByNumericCode struct {
	numericcode int
}

func (csbync *CurrencySpecificationByNumericCode) Specified(currency *Currency, i int) bool {
	return csbync.numericcode == *currency.NumericCode
}

type OrderedMapCurrencyStore struct {
	sync.Mutex

	currencies *orderedmap.OrderedMap
	nextId     int
}

func (cs *OrderedMapCurrencyStore) Add(currency *Currency) error {
	cs.Lock()
	defer cs.Unlock()

	currency.Id = cs.nextId
	cs.currencies.Set(currency.Id, *currency)
	cs.nextId++

	return nil
}

func (cs *OrderedMapCurrencyStore) Delete(currency *Currency) error {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.currencies.Delete(currency.Id)
	if !present {
		return fmt.Errorf("Currency with id=%v not found", currency.Id)
	}

	deleted := value.(Currency)
	currency.NumericCode = deleted.NumericCode
	currency.Name = deleted.Name
	currency.CharCode = deleted.CharCode
	currency.Exponent = deleted.Exponent

	return nil
}

func (cs *OrderedMapCurrencyStore) Update(currency *Currency) error {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.currencies.Get(currency.Id)
	if !present {
		return fmt.Errorf("Currency with id=%v not found", currency.Id)
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

	cs.currencies.Set(old.Id, old)

	return nil
}

func (cs *OrderedMapCurrencyStore) Query(specification CurrencySpecification) (int, []Currency) {
	cs.Lock()
	defer cs.Unlock()

	var l []Currency
	var c int = 0

	for el := cs.currencies.Oldest(); el != nil; el = el.Next() {
		currency := el.Value.(Currency)
		if specification.Specified(&currency, c) {
			l = append(l, currency)
		}
		c++
	}

	return cs.currencies.Len(), l
}

func NewOrderedMapCurrencyStore() CurrencyRepository {
	var cr CurrencyRepository

	cs := OrderedMapCurrencyStore{}
	cs.currencies = orderedmap.New()
	cs.nextId = 0

	cr = &cs

	return cr
}

func NewCurrencySpecificationByID(id int) CurrencySpecification {
	var cs CurrencySpecification

	currencySpecification := CurrencySpecificationByID{id: id}

	cs = &currencySpecification

	return cs
}

func NewCurrencySpecificationByNumericCode(numericcode int) CurrencySpecification {
	var cs CurrencySpecification

	currencySpecification := CurrencySpecificationByNumericCode{
		numericcode: numericcode,
	}

	cs = &currencySpecification

	return cs
}

func NewCurrencySpecificationWithLimitAndOffset(limit int, offset int) CurrencySpecification {
	var cs CurrencySpecification

	currencySpecification := CurrencySpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}

	cs = &currencySpecification

	return cs
}
