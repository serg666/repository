package repository

import (
	"fmt"
	"sync"
	"time"
	"github.com/wk8/go-ordered-map"
)

const (
	expireSeconds = 300
)

type SessionData map[string]interface{}

type Session struct {
	Id       *int         `json:"id"`
	Key      *string      `json:"key"`
	Data     *SessionData `json:"data"`
	ExpireAt *time.Time   `json:"expire_at"`
}

func (s Session) hasExpired() bool {
	sessionTime := *s.ExpireAt
	return sessionTime.Before(time.Now())
}

type SessionSpecification interface {
	Specified(session *Session, i int) bool
}

type SessionRepository interface {
	Add(ctx interface{}, session *Session) error
	Delete(ctx interface{}, session *Session) (error, bool)
	Update(ctx interface{}, session *Session) (error, bool)
	Query(ctx interface{}, specification SessionSpecification) (error, int, []*Session)
}

type SessionSpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (sswlao *SessionSpecificationWithLimitAndOffset) Specified(session *Session, i int) bool {
	return i >= sswlao.offset && i < sswlao.offset + sswlao.limit
}

type SessionSpecificationByID struct {
	id int
}

func (ssbyid *SessionSpecificationByID) Specified(session *Session, i int) bool {
	return ssbyid.id == *session.Id
}

type SessionSpecificationByKey struct {
	key string
}

func (ssbykey *SessionSpecificationByKey) Specified(session *Session, i int) bool {
	return ssbykey.key == *session.Key
}

type OrderedMapSessionStore struct {
	sync.Mutex

	sessions *orderedmap.OrderedMap
	nextId   int
	logger   LoggerFunc
}

func (ss *OrderedMapSessionStore) Add(ctx interface{}, session *Session) error {
	ss.Lock()
	defer ss.Unlock()

	id := ss.nextId
	expireAt := time.Now().Add(expireSeconds * time.Second)
	session.Id = &id
	session.ExpireAt = &expireAt
	ss.sessions.Set(*session.Id, *session)
	ss.nextId++

	return nil
}

func (ss *OrderedMapSessionStore) Delete(ctx interface{}, session *Session) (error, bool) {
	ss.Lock()
	defer ss.Unlock()

	value, present := ss.sessions.Delete(*session.Id)
	if !present {
		return fmt.Errorf("session with id=%v not found", *session.Id), true
	}

	deleted := value.(Session)
	session.Key = deleted.Key
	session.Data = deleted.Data
	session.ExpireAt = deleted.ExpireAt

	return nil, false
}

func (ss *OrderedMapSessionStore) Update(ctx interface{}, session *Session) (error, bool) {
	ss.Lock()
	defer ss.Unlock()

	value, present := ss.sessions.Get(*session.Id)
	if !present {
		return fmt.Errorf("session with id=%v not found", *session.Id), true
	}

	old := value.(Session)

	if session.Key != nil {
		old.Key = session.Key
	} else {
		session.Key = old.Key
	}

	if session.Data != nil {
		old.Data = session.Data
	} else {
		session.Data = old.Data
	}

	if session.ExpireAt != nil {
		old.ExpireAt = session.ExpireAt
	} else {
		session.ExpireAt = old.ExpireAt
	}

	ss.sessions.Set(*old.Id, old)

	return nil, false
}

func (ss *OrderedMapSessionStore) Query(ctx interface{}, specification SessionSpecification) (error, int, []*Session) {
	ss.Lock()
	defer ss.Unlock()

	var l []*Session
	var c int = 0

	for el := ss.sessions.Oldest(); el != nil; el = el.Next() {
		session := el.Value.(Session)
		if specification.Specified(&session, c) && !session.hasExpired() {
			l = append(l, &session)
		}
		c++
	}

	return nil, ss.sessions.Len(), l
}

func NewSession(key string, data SessionData) *Session {
	return &Session{
		Key:  &key,
		Data: &data,
	}
}

func NewOrderedMapSessionStore(
	sessions *orderedmap.OrderedMap,
	logger   LoggerFunc,
) SessionRepository {
	return &OrderedMapSessionStore{
		sessions: sessions,
		nextId:   1,
		logger:   logger,
	}
}

func NewSessionSpecificationByID(id int) SessionSpecification {
	return &SessionSpecificationByID{
		id: id,
	}
}

func NewSessionSpecificationByKey(key string) SessionSpecification {
	return &SessionSpecificationByKey{
		key: key,
	}
}

func NewSessionSpecificationWithLimitAndOffset(limit int, offset int) SessionSpecification {
	return &SessionSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}
