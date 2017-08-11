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
	Created     = "created"
	Active      = "active"
	Interrupted = "interrupted"
	Finished    = "finished"
)

// Stream type
const StreamType = "stream"

var (
	ErrStreamFinished         = errors.New("stream error: stream finished")
	ErrStreamUnknowState      = errors.New("stream error: unknow stream state")
	ErrStreamWrongStateSwitch = errors.New("stream error: wrong state switch")
	// Stream storage errors
	ErrStreamNotFound = errors.New("stream storage error: stream not found")
)

type StreamAttributes struct {
	Created string `json:"created,omitempty"`
	State   string `json:"state,omitempty"`
}

type StreamData struct {
	ID         string           `json:"id,omitempty"`
	Type       string           `json:"type"`
	Attributes StreamAttributes `json:"attributes"`
}

type Stream struct {
	Data StreamData `json:"data"`
}

type StreamList struct {
	Data []StreamData `json:"data"`
}

// NewStream creates stream with UUID and state "created"
func NewStream() Stream {
	return Stream{
		Data: StreamData{
			ID:   uuid.NewV4().String(),
			Type: StreamType,
			Attributes: StreamAttributes{
				State:   Created,
				Created: time.Now().Format(time.RFC3339),
			},
		},
	}
}

// NewStreamList creates empty stream list
func newStreamList() StreamList {
	return StreamList{
		Data: make([]StreamData, 0),
	}
}

// ToActive switches stream to active state from created/interrupted state or returns error,
// panics if state is unknown.
func (s *Stream) ToActive() error {
	switch s.Data.Attributes.State {
	case Created, Interrupted:
		s.Data.Attributes.State = Active
	case Active:
	case Finished:
		return ErrStreamFinished
	default:
		// Should never happen
		log.Panic(s.Data.Attributes.State)
	}

	return nil
}

// ToInterrupted switches stream to interrupted state from active state or returns error,
// panics if state is unknown.
func (s *Stream) ToInterrupted() error {
	switch s.Data.Attributes.State {
	case Created:
		return ErrStreamWrongStateSwitch
	case Active:
		s.Data.Attributes.State = Interrupted
	case Interrupted:
	case Finished:
		return ErrStreamFinished
	default:
		// Should never happen
		log.Panic(s.Data.Attributes.State)
	}

	return nil
}

// ToFinished swithces stream to finished modestate from active or interrupted state or returns error,
// panics if state is unknown.
func (s *Stream) ToFinished() error {
	switch s.Data.Attributes.State {
	case Created:
		return ErrStreamWrongStateSwitch
	case Active, Interrupted:
		s.Data.Attributes.State = Finished
	case Finished:
	default:
		// Should never happen
		log.Panic(s.Data.Attributes.State)
	}

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
func (ss *StreamStorage) List() (StreamList, error) {
	ss.m.RLock()
	defer ss.m.RUnlock()
	streams := newStreamList()
	for _, stream := range ss.streams {
		streams.Data = append(streams.Data, stream.Data)
	}
	return streams, nil
}

// Create creates stream and returns id
func (ss *StreamStorage) Create() (Stream, error) {
	ss.m.Lock()
	defer ss.m.Unlock()
	stream := NewStream()
	ss.streams[stream.Data.ID] = stream

	return stream, nil
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
