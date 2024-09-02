package log

import (
	"github.com/tysonmote/gommap"
	"os"
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

/*func newIndex(file *os.File, c Config) (*index, error) {

}

func (i *index) Read(offset int64) (uint64, uint64, error) {

}

func (i *index) Write(offset uint32, pos uint64) error {
}

func (i *index) Close() error {
}*/
