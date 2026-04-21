package lockset

import "sync"

type Set struct {
	mu    sync.Mutex
	locks map[string]*refMutex
}

type refMutex struct {
	mu   sync.Mutex
	refs int
}

func New() *Set {
	return &Set{locks: make(map[string]*refMutex)}
}

func (s *Set) Lock(key string) func() {
	s.mu.Lock()
	rm, ok := s.locks[key]
	if !ok {
		rm = &refMutex{}
		s.locks[key] = rm
	}
	rm.refs++
	s.mu.Unlock()

	rm.mu.Lock()
	return func() {
		rm.mu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		rm.refs--
		if rm.refs == 0 {
			delete(s.locks, key)
		}
	}
}
