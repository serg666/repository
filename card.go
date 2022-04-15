package repository

import (
	"fmt"
	"sync"
	"time"
	"regexp"
	"errors"
	"strconv"
	"strings"
	"bytes"
	"net/http"
	"io/ioutil"
	"crypto/sha256"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/durango/go-credit-card"
	"github.com/wk8/go-ordered-map"
)

const EXPIRE_DATE_FORMAT = "06/01"

type PAN string

type ExpDate struct {
	time.Time
}

func (t *ExpDate) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	t.Time, err = time.Parse(EXPIRE_DATE_FORMAT, s)
	return
}

func (s PAN) String() string {
	repeat := len(s)-4
	if repeat < 0 {
		repeat = 0
	}
	last4  := s[repeat:]
	return fmt.Sprintf("%s%s", strings.Repeat("*", repeat), string(last4))
}

type Card struct {
	Id      *int     `json:"id"`
	Token   *string  `json:"token"`
	PAN     *PAN     `json:"pan"`
	ExpDate *ExpDate `json:"exp_date"`
	Holder  *string  `json:"holder"`
}

func (c Card) String() string {
	expire := *c.ExpDate
	return fmt.Sprintf("%s (%s) <%s> [%s]", *c.PAN, expire.Format(EXPIRE_DATE_FORMAT), *c.Token, c.Type())
}

func (c Card) Type() string {
	expire := *c.ExpDate
	card := creditcard.Card{
		Number: string(*c.PAN),
		Month: expire.Month().String(),
		Year: string(expire.Year()),
	}
	err := card.Method()
	if err != nil {
		return "Unknown"
	}
	return card.Company.Short
}

type CardSpecification interface {
	Specified(card *Card, i int) bool
	ToQwrStr() string
}

type CardRepository interface {
	Add(ctx interface{}, card *Card) error
	Delete(ctx interface{}, card *Card) (error, bool)
	//Update(ctx interface{}, card *Card) (error, bool)
	Query(ctx interface{}, specification CardSpecification) (error, int, []*Card)
}

type CardSpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (cswlao *CardSpecificationWithLimitAndOffset) Specified(card *Card, i int) bool {
	return i >= cswlao.offset && i < cswlao.offset + cswlao.limit
}

func (cswlao *CardSpecificationWithLimitAndOffset) ToQwrStr() string {
	return fmt.Sprintf("?limit=%d&offset=%d", cswlao.limit, cswlao.offset)
}

type CardSpecificationByPAN struct {
	pan PAN
}

func (csbypan *CardSpecificationByPAN) Specified(card *Card, i int) bool {
	return csbypan.pan == *card.PAN
}

func (csbypan *CardSpecificationByPAN) ToQwrStr() string {
	return fmt.Sprintf("?pan=%s&limit=1", string(csbypan.pan))
}

type OrderedMapCardStore struct {
	sync.Mutex

	cards  *orderedmap.OrderedMap
	nextId int
	logger LoggerFunc
}

func generateToken(size int) (error, *string) {
	h := sha256.New()
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		return fmt.Errorf("can not generate rand bytes: %v", err), nil
	}
	h.Write(b)
	shaStr := h.Sum(nil)
	token := hex.EncodeToString(shaStr)
	return nil, &token
}

func (cs *OrderedMapCardStore) Add(ctx interface{}, card *Card) error {
	cs.Lock()
	defer cs.Unlock()

	err, token := generateToken(32)
	if err != nil {
		return fmt.Errorf("can not generate token: %v", err)
	}

	id := cs.nextId
	card.Id = &id
	card.Token = token
	cs.cards.Set(*card.Id, *card)
	cs.nextId++

	return nil
}

func (cs *OrderedMapCardStore) Delete(ctx interface{}, card *Card) (error, bool) {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.cards.Delete(*card.Id)
	if !present {
		return fmt.Errorf("card with id=%v not found", *card.Id), true
	}

	deleted := value.(Card)
	card.Token = deleted.Token
	card.PAN = deleted.PAN
	card.ExpDate = deleted.ExpDate
	card.Holder = deleted.Holder

	return nil, false
}
/*
func (cs *OrderedMapCardStore) Update(ctx interface{}, card *Card) (error, bool) {
	cs.Lock()
	defer cs.Unlock()

	value, present := cs.cards.Get(*card.Id)
	if !present {
		return fmt.Errorf("card with id=%v not found", *card.Id), true
	}

	old := value.(Card)

	if card.Token != nil {
		old.Token = card.Token
	} else {
		card.Token = old.Token
	}

	if card.PAN != nil {
		old.PAN = card.PAN
	} else {
		card.PAN = old.PAN
	}

	if card.ExpDate != nil {
		old.ExpDate = card.ExpDate
	} else {
		card.ExpDate = old.ExpDate
	}

	if card.Holder != nil {
		old.Holder = card.Holder
	} else {
		card.Holder = old.Holder
	}

	cs.cards.Set(*old.Id, old)

	return nil, false
}
*/
func (cs *OrderedMapCardStore) Query(ctx interface{}, specification CardSpecification) (error, int, []*Card) {
	cs.Lock()
	defer cs.Unlock()

	var l []*Card
	var c int = 0

	for el := cs.cards.Oldest(); el != nil; el = el.Next() {
		card := el.Value.(Card)
		if specification.Specified(&card, c) {
			l = append(l, &card)
		}
		c++
	}

	return nil, cs.cards.Len(), l
}

func NewOrderedMapCardStore(
	cards *orderedmap.OrderedMap,
	logger LoggerFunc,
) CardRepository {
	return &OrderedMapCardStore{
		cards:  cards,
		nextId: 1,
		logger: logger,
	}
}

func NewCardSpecificationByPAN(pan PAN) CardSpecification {
	return &CardSpecificationByPAN{
		pan: pan,
	}
}

func NewCardSpecificationWithLimitAndOffset(limit int, offset int) CardSpecification {
	return &CardSpecificationWithLimitAndOffset{
		limit:  limit,
		offset: offset,
	}
}

type HttpClientCardStore struct {
	url    string
	client *http.Client
	logger LoggerFunc
}

func (cs *HttpClientCardStore) maskParams(data string) string {
	sampleRegexp := regexp.MustCompile(`pan=[^&]+([^&]{4})`)
	result := sampleRegexp.ReplaceAllString(data, "pan=******$1")
	sampleRegexp = regexp.MustCompile(`"pan":"[^"]+([^"]{4})"`)
	result = sampleRegexp.ReplaceAllString(result, "\"pan\":\"******$1\"")
	return result
}

func (cs *HttpClientCardStore) makeRequest(
	ctx interface{},
	method string,
	uri string,
	contentType string,
	data string,
) (error, *map[string]interface{}, *int) {
	url := fmt.Sprintf("%s/%s", cs.url, uri)
	cs.logger(ctx).Printf("Requesting: %s", cs.maskParams(url))
	cs.logger(ctx).Printf("Params: %s", cs.maskParams(data))
	r, err := http.NewRequest(method, url, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("can not make new request: %v", err), nil, nil
	}

	r.Header.Add("Content-Type", contentType)
	r.Header.Add("Content-Length", strconv.Itoa(len(data)))

	res, err := cs.client.Do(r)
	if err != nil {
		return fmt.Errorf("can not do request: %v", err), nil, nil
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("can not read body: %v", err), nil, nil
	}

	cs.logger(ctx).Printf("response body: %s", cs.maskParams(string(body)))

	var jsonResp map[string]interface{}
	if err := json.Unmarshal(body, &jsonResp); err != nil {
		return fmt.Errorf("can not unmarshal body: %v", err), nil, nil
	}

	return nil, &jsonResp, &res.StatusCode
}

func (cs *HttpClientCardStore) Add(ctx interface{}, card *Card) error {
	pan := *card.PAN
	expire := *card.ExpDate
	var qwr = map[string]string{
		"pan": string(pan),
		"exp_date": expire.Format(EXPIRE_DATE_FORMAT),
		"holder": *card.Holder,
	}

	jsonbody, err := json.Marshal(qwr)
	if err != nil {
		return fmt.Errorf("can not marshal add card request body: %v", err)
	}

	err, jsonResp, _ := cs.makeRequest(ctx, "POST", "v1/cards", "application/json; charset=utf-8", string(jsonbody))
	if err != nil {
		return fmt.Errorf("can not make add card request: %v", err)
	}

	jsonbody, err = json.Marshal(jsonResp)
	if err != nil {
		return fmt.Errorf("can not marshal add card json response: %v", err)
	}

	d := json.NewDecoder(bytes.NewReader(jsonbody))
	if err := d.Decode(card); err != nil {
		return fmt.Errorf("can not decode add card json body response: %v", err)
	}

	return nil
}

func (cs *HttpClientCardStore) Delete(ctx interface{}, card *Card) (error, bool) {
	err, jsonResp, status := cs.makeRequest(ctx,
		"DELETE",
		fmt.Sprintf("v1/cards/%d", *card.Id),
		"application/x-www-form-urlencoded", "")

	if err != nil {
		return fmt.Errorf("cat not make delete card request: %v", err), true
	}

	if *status != 200 {
		return fmt.Errorf("failed to make delete card request. Http status: %d", *status), true
	}

	jsonbody, err := json.Marshal(jsonResp)
	if err != nil {
		return fmt.Errorf("can not marshal delete card json response: %v", err), true
	}

	d := json.NewDecoder(bytes.NewReader(jsonbody))
	if err := d.Decode(card); err != nil {
		return fmt.Errorf("can not decode delete card json body response: %v", err), true
	}

	return nil, false
}

func (cs *HttpClientCardStore) Query(ctx interface{}, specification CardSpecification) (error, int, []*Card) {
	var l []*Card
	var c int = 0

	err, jsonResp, _ := cs.makeRequest(ctx, "GET", fmt.Sprintf(
		"v1/cards%s",
		specification.ToQwrStr()),
	"application/x-www-form-urlencoded", "")
	if err != nil {
		return fmt.Errorf("can not make query card request: %v", err), c, l
	}

	if data, ok := (*jsonResp)["data"]; ok {
		if rows, ok := data.([]interface{}); ok {
			for _, row := range rows {
				jsonbody, err := json.Marshal(&row)
				if err != nil {
					return fmt.Errorf("can not marshal query card list json response: %v", err), c, l
				}

				var card Card
				d := json.NewDecoder(bytes.NewReader(jsonbody))
				if err := d.Decode(&card); err != nil {
					return fmt.Errorf("can not decode query card list json body response: %v", err), c, l
				}
				l = append(l, &card)
			}
		} else {
			return errors.New("card query response list has wrong type"), c, l
		}
	} else {
		return errors.New("card query response has no list"), c, l
	}

	return nil, c, l
}

func NewHttpClientCardStore(
	url string,
	client *http.Client,
	logger LoggerFunc,
) CardRepository {
	return &HttpClientCardStore{
		url:    url,
		client: client,
		logger: logger,
	}
}
