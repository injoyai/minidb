package core

import (
	"bufio"
	"github.com/injoyai/conv"
	"io"
	"os"
	"sync"
)

func NewFile(filename string, writeCacheSize ...int) *File {
	return &File{
		Filename:       filename,
		writeCacheSize: conv.DefaultInt(0, writeCacheSize...),
		Split:          []byte{' ', 0xFF, '\n'},
	}
}

type File struct {
	Filename       string                             //文件名称
	writeCacheSize int                                //缓存大小会大于等于设置的值,为0表示实时写入
	mu             sync.RWMutex                       //锁
	OpenFunc       func(s *Scanner) ([][]byte, error) //
	Split          []byte                             //每条数据的分隔符
}

func (this *File) NewScanner(r io.Reader) *Scanner {
	return NewScanner(r, this.Split)
}

func (this *File) OnOpen(f func(s *Scanner) ([][]byte, error)) {
	this.OpenFunc = f
}

func (this *File) Limit(search func(i int, bs []byte) (any, bool), size int, offset ...int) ([]any, error) {
	index := 0
	result := []any(nil)
	this.Range(func(i int, bs []byte) bool {
		v, ok := search(i, bs)
		if ok {
			//附和预期的数据
			index++
			//进行分页
			if len(offset) > 0 && index <= offset[0] {
				return true
			}

			switch {
			case size < 0:
				result = append(result, v)
			case len(result) < size:
				result = append(result, v)
				if len(result) == size {
					return false
				}
			default:
				return false
			}
		}
		return true
	})
	return result, nil
}

func (this *File) Range(fn func(i int, bs []byte) bool) error {

	this.mu.RLock()
	defer this.mu.RUnlock()

	f, err := os.Open(this.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := this.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		if !fn(i, scanner.Bytes()) {
			break
		}
	}
	return scanner.Err()
}

func (this *File) Append(p []byte) error {
	return this.AppendWith(func(s *Scanner) ([][]byte, error) {
		return [][]byte{p}, nil
	})
}

func (this *File) AppendWith(fn func(s *Scanner) ([][]byte, error)) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	f, err := os.OpenFile(this.Filename, os.O_RDWR, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := this.NewScanner(f)

	if this.OpenFunc != nil {
		_, err := this.OpenFunc(scanner)
		if err != nil {
			return err
		}
	}

	data, err := fn(scanner)
	if err != nil {
		return err
	}

	if _, err := f.Seek(0, 2); err != nil {
		return err
	}

	for _, bs := range data {
		p := append(bs, this.Split...)
		if _, err = f.Write(p); err != nil {
			return err
		}
	}

	return nil
}

func (this *File) Insert(index int, data []byte) error {
	return this.Update(func(i int, bs []byte) ([][]byte, error) {
		if index != i {
			return [][]byte{bs}, nil
		}
		return [][]byte{data, bs}, nil
	})
}

func (this *File) Update(fn func(i int, bs []byte) ([][]byte, error)) (err error) {
	//加锁,防止并发被覆盖
	this.mu.Lock()
	defer this.mu.Unlock()

	//临时文件名称
	tempFilename := this.Filename + ".temp"

	defer func() {
		if err == nil {
			//重命名临时文件到源文件
			err = os.Rename(tempFilename, this.Filename)
		}
	}()

	//打开源文件
	f, err := os.Open(this.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	//新建临时文件
	tempFile, err := os.Create(tempFilename)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	scanner := this.NewScanner(f)

	//打开事件
	if this.OpenFunc != nil {
		ls, err := this.OpenFunc(scanner)
		if err != nil {
			return err
		}
		if err := this.write(writer, ls...); err != nil {
			return err
		}
	}

	for i := 0; scanner.Scan(); i++ {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		replaces, err := fn(i, scanner.Bytes())
		if err != nil {
			return err
		}
		if replaces == nil {
			continue
		}
		if err := this.write(writer, replaces...); err != nil {
			return err
		}
	}

	//写入磁盘,减少写入次数
	if err = writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (this *File) Del(index int) (err error) {
	return this.Update(func(i int, bs []byte) ([][]byte, error) {
		if index == i {
			return nil, nil
		}
		return [][]byte{bs}, nil
	})
}

func (this *File) DelBy(fn func(i int, bs []byte) (del bool, err error)) (err error) {
	return this.Update(func(i int, bs []byte) ([][]byte, error) {
		del, err := fn(i, bs)
		if err != nil {
			return nil, err
		}
		if del {
			return nil, nil
		}
		return [][]byte{bs}, nil
	})
}

func (this *File) write(w *bufio.Writer, data ...[]byte) error {
	for _, bs := range data {
		if _, err := w.Write(bs); err != nil {
			return err
		}
		if _, err := w.Write(this.Split); err != nil {
			return err
		}
		if w.Size() >= this.writeCacheSize {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}
