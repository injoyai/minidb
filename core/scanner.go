package core

import (
	"bufio"
	"bytes"
	"io"
)

func NewScanner(r io.Reader, split []byte) *Scanner {
	s := &Scanner{
		Scanner: bufio.NewScanner(r),
	}
	s.Scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if n := bytes.Index(data, split); n >= 0 {
			return n + len(split), data[:n], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})
	return s
}

type Scanner struct {
	//会缓存大量数据在buf中,导致后续读取不到数据
	*bufio.Scanner
}

func (this *Scanner) Range(fn func(i int, bs []byte) (bool, error)) error {
	for i := 0; this.Scanner.Scan(); i++ {
		ok, err := fn(i, this.Scanner.Bytes())
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	return this.Err()
}

func (this *Scanner) LimitBytes(size int, offset ...int) ([][]byte, error) {
	ls := [][]byte(nil)
	_, err := this.Limit(func(i int, bs []byte) (any, bool) {
		ls = append(ls, bs)
		return struct{}{}, true
	}, size, offset...)
	return ls, err
}

func (this *Scanner) Limit(search func(i int, bs []byte) (any, bool), size int, offset ...int) ([]any, error) {

	if search == nil {
		search = func(i int, bs []byte) (any, bool) {
			return bs, true
		}
	}

	index := 0
	var result []any
	err := this.Range(func(i int, bs []byte) (bool, error) {
		v, ok := search(i, bs)
		if ok {
			//附和预期的数据
			index++
			//进行分页
			if len(offset) > 0 && index <= offset[0] {
				return true, nil
			}

			switch {
			case size < 0:
				result = append(result, v)
			case len(result) < size:
				result = append(result, v)
				if len(result) == size {
					return false, nil
				}
			default:
				return false, nil
			}
		}
		return true, nil
	})

	return result, err
}
