package core

import (
	"bufio"
	"bytes"
	"github.com/injoyai/conv"
	"os"
	"sync"
)

func NewFile(filename string, writeCacheSize ...int) *File {
	return &File{
		Filename:       filename,
		writeCacheSize: conv.DefaultInt(0, writeCacheSize...),
	}
}

type File struct {
	Filename       string       //文件名称
	writeCacheSize int          //缓存大小会大于等于设置的值,为0表示实时写入
	mu             sync.RWMutex //锁
	openFunc       func(s *Scanner) ([][]byte, error)
}

func (this *File) OnOpen(f func(s *Scanner) ([][]byte, error)) {
	this.openFunc = f
}

func (this *File) split() []byte {
	//预留,方便拓展其他分隔符
	return []byte{'\n'}
}

func (this *File) escapeSplit() []byte {
	//预留,方便拓展其他分隔符
	return []byte(`\\n`)
}

func (this *File) escape(bs []byte) []byte {
	return bytes.ReplaceAll(bs, this.split(), this.escapeSplit())
}

func (this *File) unescape(bs []byte) []byte {
	return bytes.ReplaceAll(bs, this.escapeSplit(), this.split())
}

func (this *File) Limit(fn func(i int, bs []byte) (any, bool), size int, offset ...int) ([]any, error) {
	index := 0
	result := []any(nil)
	this.Range(func(i int, bs []byte) bool {
		v, ok := fn(i, bs)
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

	scanner := bufio.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		if !fn(i, this.unescape(scanner.Bytes())) {
			break
		}
	}
	return nil
}

func (this *File) Append(p []byte) error {
	return this.AppendWith(func(s *Scanner) ([][]byte, error) {
		return [][]byte{p}, nil
	})
}

func (this *File) AppendWith(fn func(s *Scanner) ([][]byte, error)) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	f, err := os.OpenFile(this.Filename, os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := NewScanner(f)

	if this.openFunc != nil {
		_, err := this.openFunc(scanner)
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
		p := append(this.escape(bs), this.split()...)
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
	scanner := NewScanner(f)

	//打开事件
	if this.openFunc != nil {
		ls, err := this.openFunc(scanner)
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
		replaces, err := fn(i, this.unescape(scanner.Bytes()))
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
		if _, err := w.Write(this.escape(bs)); err != nil {
			return err
		}
		if _, err := w.Write(this.split()); err != nil {
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
