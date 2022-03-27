package repository

import (
	"fmt"
	"sync"
	"github.com/wk8/go-ordered-map"
)

type Profile struct {
	Id          *int
	Key         *string
	Description *string
	Currency    *Currency
}

type ProfileSpecification interface {
	Specified(profile *Profile, i int) bool
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

type ProfileSpecificationByID struct {
	id int
}

func (psbyid *ProfileSpecificationByID) Specified(profile *Profile, i int) bool {
	return psbyid.id == *profile.Id
}

type ProfileSpecificationByKey struct {
	key string
}

func (psbykey *ProfileSpecificationByKey) Specified(profile *Profile, i int) bool {
	return psbykey.key == *profile.Key
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
