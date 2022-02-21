package repository

import (
	"fmt"
	"sync"
	"github.com/wk8/go-ordered-map"
)

type Profile struct {
	Id          int
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
	Query(ctx interface{}, specification ProfileSpecification) (error, int, []Profile)
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
	return psbyid.id == profile.Id
}

type OrderedMapProfileStore struct {
	sync.Mutex

	profiles *orderedmap.OrderedMap
	nextId   int
	logger   LoggerFunc
}

func (ps *OrderedMapProfileStore) Add(ctx interface{}, profile *Profile) error {
	ps.Lock()
	defer ps.Unlock()

	profile.Id = ps.nextId
	ps.profiles.Set(profile.Id, *profile)
	ps.nextId++

	return nil
}

func (ps *OrderedMapProfileStore) Delete(ctx interface{}, profile *Profile) (error, bool) {
	ps.Lock()
	defer ps.Unlock()

	value, present := ps.profiles.Delete(profile.Id)
	if !present {
		return fmt.Errorf("profile with id=%v not found", profile.Id), true
	}

	deleted := value.(Profile)
	profile.Key = deleted.Key
	profile.Description = deleted.Description
	profile.Currency = deleted.Currency

	return nil, false
}

func (ps *OrderedMapProfileStore) Update(ctx interface{}, profile *Profile) (error, bool) {
	ps.Lock()
	defer ps.Unlock()

	value, present := ps.profiles.Get(profile.Id)
	if !present {
		return fmt.Errorf("profile with id=%v not found", profile.Id), true
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

	ps.profiles.Set(old.Id, old)

	return nil, false
}

func (ps *OrderedMapProfileStore) Query(ctx interface{}, specification ProfileSpecification) (error, int, []Profile) {
	ps.Lock()
	defer ps.Unlock()

	var l []Profile
	var c int = 0

	ps.logger(ctx).Print("some message")
	for el := ps.profiles.Oldest(); el != nil; el = el.Next() {
		profile := el.Value.(Profile)
		if specification.Specified(&profile, c) {
			l = append(l, profile)
		}
		c++
	}

	return nil, ps.profiles.Len(), l
}

func NewOrderedMapProfileStore(logger LoggerFunc) ProfileRepository {
	return &OrderedMapProfileStore{
		profiles: orderedmap.New(),
		nextId:   0,
		logger:   logger,
	}
}

func NewProfileSpecificationByID(id int) ProfileSpecification {
	return &ProfileSpecificationByID{
		id: id,
	}
}

func NewProfileSpecificationWithLimitAndOffset(limit int, offset int) ProfileSpecification {
	return &ProfileSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}
