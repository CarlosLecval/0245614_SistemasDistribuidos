package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: uint64(fileInfo.Size()),
	}, nil
}

func (s *store) Append(value []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := binary.Write(s.buf, enc, uint64(len(value))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(value)
	if err != nil {
		return 0, 0, err
	}
	if w != len(value) {
		return 0, 0, err
	}
	pos := s.size
	writtenBytes := uint64(lenWidth + w)
	s.size += writtenBytes
	return writtenBytes, pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
