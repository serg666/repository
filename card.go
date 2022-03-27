package repository

import (
	"fmt"
	"sync"
	"time"
	"strings"
	"crypto/sha256"
	"crypto/rand"
	"encoding/hex"
	"github.com/durango/go-credit-card"
	"github.com/wk8/go-ordered-map"
)

type PAN string

func (s PAN) String() string {
	repeat := len(s)-4
	if repeat < 0 {
		repeat = 0
	}
	last4  := s[repeat:]
	return fmt.Sprintf("%s%s", strings.Repeat("*", repeat), string(last4))
}

type Card struct {
	Id      *int
	Token   *string
	PAN     *PAN
	ExpDate *time.Time
	Holder  *string
}

func (c Card) String() string {
	expire := *c.ExpDate
	return fmt.Sprintf("%s (%s) <%s> [%s]", *c.PAN, expire.Format("2006/01"), *c.Token, c.Type())
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
}

type CardRepository interface {
	Add(ctx interface{}, card *Card) error
	Delete(ctx interface{}, card *Card) (error, bool)
	Update(ctx interface{}, card *Card) (error, bool)
	Query(ctx interface{}, specification CardSpecification) (error, int, []*Card)
}

type CardSpecificationWithLimitAndOffset struct {
	limit int
	offset int
}

func (cswlao *CardSpecificationWithLimitAndOffset) Specified(card *Card, i int) bool {
	return i >= cswlao.offset && i < cswlao.offset + cswlao.limit
}

type CardSpecificationByPAN struct {
	pan PAN
}

func (csbypan *CardSpecificationByPAN) Specified(card *Card, i int) bool {
	return csbypan.pan == *card.PAN
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
