package StreamStorage

import (
	"errors"
	"sync"
	"time"

	"github.com/labstack/gommon/log"
	uuid "github.com/satori/go.uuid"
)

// Stream states
const (
	created     = "created"
	active      = "active"
	interrupted = "interrupted"
	finished    = "finished"
)

// Errors
var (
	// Stream errors
	ErrStreamAlreadyActive      = errors.New("stream: stream already active")
	ErrStreamAlreadyInterrupted = errors.New("stream: stream already interrupted")
	ErrStreamFinished           = errors.New("stream: stream finished")
	ErrStreamIsNotActive        = errors.New("stream: stream is not active")
	ErrStreamIsNotInterrupted   = errors.New("stream: stream is not interrupted")
	// Stream storage errors
	ErrStreamNotFound = errors.New("stream storage: stream not found")
)

type Stream struct {
	Created string `json:"created"`
	ID      string `json:"id"`
	State   string `json:"state"`
}

// NewStream creates stream with UUID and state "created"
func NewStream() Stream {
	return Stream{
		State:   created,
		Created: time.Now().Format(time.RFC3339),
		ID:      uuid.NewV4().String(),
	}
}

// ToActive switches stream to active state from created/interrupted state or returns error,
// panics if state is unknown.
func (s *Stream) ToActive() error {
	switch s.State {
	case created:
		s.State = active
	case active:
		return ErrStreamAlreadyActive
	case interrupted:
		s.State = active
	case finished:
		return ErrStreamFinished
	default:
		// Should never happen
		log.Panic(s.State)
	}

	return nil
}

// ToInterrupted switches stream to interrupted state from active state or returns error,
// panics if state is unknown.
func (s *Stream) ToInterrupted() error {
	switch s.State {
	case created:
		return ErrStreamIsNotActive
	case active:
		s.State = interrupted
	case interrupted:
		return ErrStreamAlreadyInterrupted
	case finished:
		return ErrStreamFinished
	default:
		// Should never happen
		log.Panic(s.State)
	}

	return nil
}

// ToFinished swithces stream to finished modestate from interrupted state in other states returns error.
func (s *Stream) ToFinished() error {
	if s.State != interrupted {
		return ErrStreamIsNotInterrupted
	}
	s.State = finished

	return nil
}

// StreamStorage store streams.
// Field StreamChan contains UUIDs of interrupted streams and channels for communication with streams goroutines.
// Field Streams contains UUID as key and Stream objects as value.
type StreamStorage struct {
	streamsChan map[string]chan struct{}
	streams     map[string]Stream
	m           *sync.RWMutex
	timeout     time.Duration
}

// NewStreamStorage returns StreamStorage.
// Parameter timeout it's a time which Stream will be in "interrupted" mode before switch to "finished" mode.
func NewStreamStorage(timeout time.Duration) StreamStorage {
	return StreamStorage{
		streamsChan: make(map[string]chan struct{}),
		streams:     make(map[string]Stream),
		m:           &sync.RWMutex{},
		timeout:     timeout,
	}
}

// List returns streams list
func (ss *StreamStorage) List() ([]Stream, error) {
	ss.m.RLock()
	defer ss.m.RUnlock()
	streams := make([]Stream, 0)
	for _, stream := range ss.streams {
		streams = append(streams, stream)
	}
	return streams, nil
}

// Create creates stream and returns id
func (ss *StreamStorage) Create() (string, error) {
	ss.m.Lock()
	defer ss.m.Unlock()
	stream := NewStream()
	ss.streams[stream.ID] = stream

	return stream.ID, nil
}

// Get returns stream to client
func (ss *StreamStorage) Get(ID string) (Stream, error) {
	ss.m.RLock()
	defer ss.m.RUnlock()
	if stream, ok := ss.streams[ID]; ok {
		return stream, nil
	}

	return Stream{}, ErrStreamNotFound
}

// Run switches stream to "active" state
func (ss *StreamStorage) Run(ID string) error {
	ss.m.Lock()
	defer ss.m.Unlock()
	stream, ok := ss.streams[ID]
	if !ok {
		return ErrStreamNotFound
	}

	if err := stream.ToActive(); err != nil {
		return err
	}

	// Stop goroutine if previous state was "interrupted"
	if ch, ok := ss.streamsChan[ID]; ok {
		delete(ss.streamsChan, ID)
		close(ch)
	}

	ss.streams[ID] = stream
	return nil
}

// Stop switches stream to "interrupted" state
func (ss *StreamStorage) Stop(ID string) error {
	ss.m.Lock()
	defer ss.m.Unlock()
	stream, ok := ss.streams[ID]

	if !ok {
		return ErrStreamNotFound
	}

	if err := stream.ToInterrupted(); err != nil {
		return err
	}

	// Run goroutine with timer
	ch := make(chan struct{})
	go func(ss *StreamStorage, ID string, stop chan struct{}) {
		select {
		case <-time.After(ss.timeout):
			ss.Finish(ID)
		case <-stop:
		}
	}(ss, ID, ch)
	ss.streamsChan[ID] = ch

	ss.streams[ID] = stream
	return nil
}

// Finish switches stream to "finished" state
func (ss *StreamStorage) Finish(ID string) error {
	ss.m.Lock()
	defer ss.m.Unlock()
	stream, ok := ss.streams[ID]

	if !ok {
		return ErrStreamNotFound
	}

	if err := stream.ToFinished(); err != nil {
		return err
	}

	// Close channel if exist
	if ch, ok := ss.streamsChan[ID]; ok {
		delete(ss.streamsChan, ID)
		close(ch)
	}

	ss.streams[ID] = stream
	return nil
}
