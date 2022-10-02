package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
)

const (
	defaultMaxHeight = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {

	header := &Element{
		levels: make([]*Element, defaultMaxHeight),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxHeight - 1,
		rand:     r,
	}
}

type Element struct {
	// levels[i] 存的是这个节点的第 i 个 level 的下一个节点
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level+1),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(newData *codec.Entry) error {
	//implement me here!!!
	list.lock.Lock()
	defer list.lock.Unlock()

	prevElementsOfNewData := make([]*Element, list.maxLevel+1) // to save the previous elements of all levels of new one

	newKey := newData.Key
	newKeyScore := list.calcScore(newKey)
	header, maxLevel := list.header, list.maxLevel
	prev := header

	// find inserting position
	for i := maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(newKeyScore, newKey, next); comp <= 0 {
				if comp == 0 {
					// update operation rather than insert new one
					next.entry = newData
					return nil
				} else { // new value is greater than next value
					prev = next
				}
			} else {
				// new value must be between the prev and next element
				// found the correct interval for the current level, we need to break and move to the next level (-1)
				break
			}
		}
		prevElementsOfNewData[i] = prev
	}

	randomMaxLevelForNewData, newKeyScore := list.randLevel(), list.calcScore(newKey)
	currElement := newElement(newKeyScore, newData, randomMaxLevelForNewData)

	// start inserting
	for i := randomMaxLevelForNewData; i >= 0; i-- {
		nextElement := prevElementsOfNewData[i].levels[i]
		prevElementsOfNewData[i].levels[i] = currElement
		currElement.levels[i] = nextElement
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!

	list.lock.RLock()
	defer list.lock.RUnlock()
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = prev.levels[i] {
			if comp := list.compare(keyScore, key, next); comp <= 0 {
				if comp == 0 {
					return next.entry
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - (i+1)*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)

	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	// 有 1/2 的几率返回 1
	// 有 1/4 的几率返回 2
	// 有 1/8 的几率返回 3
	// 直到最大层
	for i := 0; i < list.maxLevel; i++ {
		if list.rand.Intn(2) == 0 {
			return i
		}
	}

	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
