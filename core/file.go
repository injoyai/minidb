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
		writeCacheSize: conv.Default(0, writeCacheSize...),
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

func (this *File) WithScanner(fn func(f *os.File, p [][]byte, s *Scanner) error) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	file, err := os.OpenFile(this.Filename, os.O_RDWR, 0o666)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := NewScanner(file, this.Split)

	prefix := [][]byte(nil)
	if this.OpenFunc != nil {
		prefix, err = this.OpenFunc(scanner)
		if err != nil {
			return err
		}
	}
	return fn(file, prefix, scanner)
}

func (this *File) OnOpen(f func(s *Scanner) ([][]byte, error)) {
	this.OpenFunc = f
}

func (this *File) Limit(search func(i int, bs []byte) (any, bool), size int, offset ...int) (result []any, err error) {
	err = this.WithScanner(func(f *os.File, p [][]byte, s *Scanner) error {
		result, err = s.Limit(search, size, offset...)
		return err
	})
	return
}

// Range 遍历数据,不包括被消费(OnOpen)的数据
func (this *File) Range(fn func(i int, bs []byte) bool) error {
	return this.WithScanner(func(f *os.File, p [][]byte, s *Scanner) error {
		return s.Range(func(i int, bs []byte) (bool, error) {
			return fn(i, bs), nil
		})
	})
}

// Append 追加数据,对应orm的Insert
func (this *File) Append(data ...[]byte) error {
	return this.WithScanner(func(f *os.File, p [][]byte, s *Scanner) error {
		if _, err := f.Seek(0, 2); err != nil {
			return err
		}
		for _, bs := range data {
			ls := append(bs, this.Split...)
			if _, err := f.Write(ls); err != nil {
				return err
			}
		}
		return nil
	})
}

func (this *File) AppendWith(fn func() ([][]byte, error)) error {
	return this.WithScanner(func(f *os.File, p [][]byte, s *Scanner) error {
		if _, err := f.Seek(0, 2); err != nil {
			return err
		}
		data, err := fn()
		if err != nil {
			return err
		}
		for _, bs := range data {
			ls := append(bs, this.Split...)
			if _, err := f.Write(ls); err != nil {
				return err
			}
		}
		return nil
	})
}

// Insert 插入数据,其实就是更新数据,变成多条数据
func (this *File) Insert(index int, data []byte) error {
	return this.Update(func(i int, bs []byte) ([][]byte, error) {
		if index != i {
			return [][]byte{bs}, nil
		}
		return [][]byte{data, bs}, nil
	})
}

// Update 更新数据
func (this *File) Update(fn func(i int, bs []byte) ([][]byte, error)) (err error) {
	//临时文件名称
	tempFilename := this.Filename + ".temp"
	defer func() {
		if err == nil {
			//重命名临时文件到源文件
			err = os.Rename(tempFilename, this.Filename)
		}
	}()

	return this.WithScanner(func(f *os.File, p [][]byte, s *Scanner) error {

		//新建临时文件
		tempFile, err := os.Create(tempFilename)
		if err != nil {
			return err
		}
		defer tempFile.Close()

		writer := bufio.NewWriter(tempFile)
		if err := this.write(writer, p...); err != nil {
			return err
		}

		err = s.Range(func(i int, bs []byte) (bool, error) {
			replaces, err := fn(i, bs)
			if err != nil {
				return false, err
			}
			if replaces == nil {
				return true, nil
			}
			if err := this.write(writer, replaces...); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return err
		}

		//写入磁盘,减少写入次数
		return writer.Flush()
	})
}

// Del 按行数删除
func (this *File) Del(index int) (err error) {
	return this.DelBy(func(i int, bs []byte) (del bool, err error) {
		if index == i {
			return true, nil
		}
		return false, nil
	})
}

// DelBy 按条件删除
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

// write 写入数据,附带分隔符
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
