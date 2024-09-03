package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth uint64 = offWidth + posWidth
)

type index struct {
	*os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(file *os.File, c Config) (*index, error) {
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	file.Truncate(int64(c.Segment.MaxIndexBytes))
	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &index{
		File: file,
		mmap: mmap,
		size: uint64(fi.Size()),
	}, nil
}

func (i *index) Read(wantedOffset int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if wantedOffset < 0 {
		wantedOffset = int64(i.size/entWidth) + wantedOffset
	}

	if wantedOffset < 0 {
		return 0, 0, io.EOF
	}

	wantedPos := uint64(wantedOffset) * entWidth

	if wantedPos >= i.size {
		return 0, 0, io.EOF
	}

	offset := enc.Uint32(i.mmap[wantedPos : wantedPos+offWidth])
	pos := enc.Uint64(i.mmap[wantedPos+offWidth : wantedPos+entWidth])
	return offset, pos, nil
}

func (i *index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap))-i.size < entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += entWidth

	return nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.File.Close()
}
