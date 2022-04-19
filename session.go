package repository

import (
	"fmt"
	"sync"
	"time"
	"bytes"
	"errors"
	"strconv"
	"strings"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/wk8/go-ordered-map"
)

const (
	expireSeconds = 300
)

type SessionData map[string]interface{}

type Session struct {
	Id       *int         `json:"id"`
	Key      *string      `json:"key"`
	Data     *SessionData `json:"body"`
	ExpireAt *time.Time   `json:"expire_at"`
}

func (s Session) hasExpired() bool {
	sessionTime := *s.ExpireAt
	return sessionTime.Before(time.Now())
}

type SessionSpecification interface {
	Specified(session *Session, i int) bool
	ToQwrStr() string
}

type SessionRepository interface {
	Add(ctx interface{}, session *Session) error
	//Delete(ctx interface{}, session *Session) (error, bool)
	//Update(ctx interface{}, session *Session) (error, bool)
	Query(ctx interface{}, specification SessionSpecification) (error, int, []*Session)
}
/*
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
*/
type SessionSpecificationByKey struct {
	key string
}

func (ssbykey *SessionSpecificationByKey) Specified(session *Session, i int) bool {
	return ssbykey.key == *session.Key
}

func (ssbykey *SessionSpecificationByKey) ToQwrStr() string {
	return fmt.Sprintf("/%s", ssbykey.key)
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
/*
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
*/
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

func NewSessionSpecificationByKey(key string) SessionSpecification {
	return &SessionSpecificationByKey{
		key: key,
	}
}
/*
func NewSessionSpecificationByID(id int) SessionSpecification {
	return &SessionSpecificationByID{
		id: id,
	}
}

func NewSessionSpecificationWithLimitAndOffset(limit int, offset int) SessionSpecification {
	return &SessionSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}
*/
type HttpClientSessionStore struct {
	url    string
	client *http.Client
	logger LoggerFunc
}

func (ss *HttpClientSessionStore) makeRequest(
	ctx interface{},
	method string,
	uri string,
	contentType string,
	data string,
) (error, *map[string]interface{}, *int) {
	url := fmt.Sprintf("%s/%s", ss.url, uri)
	r, err := http.NewRequest(method, url, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("can not make new request: %v", err), nil, nil
	}

	r.Header.Add("Content-Type", contentType)
	r.Header.Add("Content-Length", strconv.Itoa(len(data)))

	res, err := ss.client.Do(r)
	if err != nil {
		return fmt.Errorf("can not do request: %v", err), nil, nil
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("can not read body: %v", err), nil, nil
	}

	var jsonResp map[string]interface{}
	if err := json.Unmarshal(body, &jsonResp); err != nil {
		return fmt.Errorf("can not unmarshal body: %v", err), nil, nil
	}

	return nil, &jsonResp, &res.StatusCode
}

func (ss *HttpClientSessionStore) unmarshalSessionData(jsonResp *map[string]interface{}) error {
	if body, ok := (*jsonResp)["body"]; ok {
		if bodyStr, ok := body.(string); ok {
			var sessionData SessionData
			if err := json.Unmarshal([]byte(bodyStr), &sessionData); err != nil {
				return fmt.Errorf("can not unmarshal session body json response: %v", err)
			}
			(*jsonResp)["body"] = sessionData
		}
	}

	return nil
}

func (ss *HttpClientSessionStore) Add(ctx interface{}, session *Session) error {
	body, err := json.Marshal(session.Data)
	if err != nil {
		return fmt.Errorf("can not marshal session data: %v", err)
	}

	var qwr = map[string]string{
		"key": *session.Key,
		"body": string(body),
	}

	jsonbody, err := json.Marshal(qwr)
	if err != nil {
		return fmt.Errorf("can not marshal add session request body: %v", err)
	}

	err, jsonResp, _ := ss.makeRequest(ctx, "POST", "v1/sessions", "application/json; charset=utf-8", string(jsonbody))
	if err != nil {
		return fmt.Errorf("can not make add session request: %v", err)
	}

	if err := ss.unmarshalSessionData(jsonResp); err != nil {
		return fmt.Errorf("can not unmarshal added session data: %v", err)
	}

	jsonbody, err = json.Marshal(jsonResp)
	if err != nil {
		return fmt.Errorf("can not marshal add session json response: %v", err)
	}

	d := json.NewDecoder(bytes.NewReader(jsonbody))
	if err := d.Decode(session); err != nil {
		return fmt.Errorf("can not decode add session json body response: %v", err)
	}

	return nil
}

func (ss *HttpClientSessionStore) appendToList (l *[]*Session, data *map[string]interface{}) error {
	if err := ss.unmarshalSessionData(data); err != nil {
		return fmt.Errorf("can not unmarshal query session data: %v", err)
	}

	jsonbody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("can not marshal query session list json response: %v", err)
	}

	var session Session
	d := json.NewDecoder(bytes.NewReader(jsonbody))
	if err := d.Decode(&session); err != nil {
		return fmt.Errorf("can not decode query session list json body response: %v", err)
	}
	*l = append(*l, &session)

	return nil
}

func (ss *HttpClientSessionStore) Query(ctx interface{}, specification SessionSpecification) (error, int, []*Session) {
	var l []*Session
	var c int = 0

	err, jsonResp, _ := ss.makeRequest(ctx, "GET", fmt.Sprintf(
		"v1/sessions%s",
		specification.ToQwrStr()),
	"application/x-www-form-urlencoded", "")
	if err != nil {
		return fmt.Errorf("can not make query session request: %v", err), c, l
	}

	if data, ok := (*jsonResp)["data"]; ok {
		if rows, ok := data.([]interface{}); ok {
			for _, row := range rows {
				if r, ok := row.(map[string]interface{}); ok {
					if err := ss.appendToList(&l, &r); err != nil {
						return fmt.Errorf("can not append to list: %v", err), c, l
					}
				} else {
					return errors.New("session query response list has wrong type"), c, l
				}
			}
		} else {
			return errors.New("session query response list has wrong type"), c, l
		}
	} else {
		if err := ss.appendToList(&l, jsonResp); err != nil {
			return fmt.Errorf("can not append to list: %v", err), c, l
		}
	}

	return nil, c, l
}

func NewHttpClientSessionStore(
	url string,
	client *http.Client,
	logger LoggerFunc,
) SessionRepository {
	return &HttpClientSessionStore{
		url:    url,
		client: client,
		logger: logger,
	}
}
