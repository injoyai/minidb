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
		filename:       filename,
		writeCacheSize: conv.DefaultInt(0, writeCacheSize...),
	}
}

type File struct {
	filename       string       //文件名称
	writeCacheSize int          //缓存大小会大于等于设置的值,为0表示实时写入
	mu             sync.RWMutex //锁
}

func (this *File) getSplit() []byte {
	//预留,方便拓展其他分隔符
	return []byte{'\n'}
}

func (this *File) escape(bs []byte) []byte {
	return bytes.ReplaceAll(bs, []byte{'\n'}, []byte(`\\n`))
}

func (this *File) unescape(bs []byte) []byte {
	return bytes.ReplaceAll(bs, []byte(`\\n`), []byte{'\n'})
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

	f, err := os.Open(this.filename)
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

	this.mu.Lock()
	defer this.mu.Unlock()

	f, err := os.OpenFile(this.filename, os.O_APPEND, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()

	p = append(this.escape(p), this.getSplit()...)
	_, err = f.Write(p)
	return err
}

func (this *File) Insert(index int, data []byte) error {
	return this.Update(func(i int, bs []byte) [][]byte {
		if index != i {
			return [][]byte{bs}
		}
		return [][]byte{data, bs}
	})
}

func (this *File) Update(fn func(i int, bs []byte) [][]byte) (err error) {
	//加锁,防止并发被覆盖
	this.mu.Lock()
	defer this.mu.Unlock()

	//临时文件名称
	tempFilename := this.filename + ".temp"

	defer func() {
		if err == nil {
			//重命名临时文件到源文件
			err = os.Rename(tempFilename, this.filename)
		}
	}()

	//打开源文件
	f, err := os.Open(this.filename)
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

	scanner := bufio.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		replaces := fn(i, this.unescape(scanner.Bytes()))
		if replaces == nil {
			continue
		}
		for _, replace := range replaces {
			_, err = writer.Write(this.escape(replace))
			if err != nil {
				return err
			}
			_, err = writer.Write(this.getSplit())
			if err != nil {
				return err
			}
			if writer.Size() >= this.writeCacheSize {
				if err = writer.Flush(); err != nil {
					return err
				}
			}
		}
	}

	//写入磁盘,减少写入次数
	if err = writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (this *File) Del(index int) (err error) {
	return this.Update(func(i int, bs []byte) [][]byte {
		if index == i {
			return nil
		}
		return [][]byte{bs}
	})
}

func (this *File) DelBy(fn func(i int, bs []byte) bool) (err error) {
	return this.Update(func(i int, bs []byte) [][]byte {
		if fn(i, bs) {
			return nil
		}
		return [][]byte{bs}
	})
}
