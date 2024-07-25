package core

import (
	"bufio"
	"github.com/injoyai/conv"
	"os"
)

func NewScanner(f *os.File) *Scanner {
	return &Scanner{
		Scanner: bufio.NewScanner(f),
	}
}

type Scanner struct {
	//会缓存大量数据在buf中,导致后续读取不到数据
	*bufio.Scanner
}

func (this *Scanner) LimitBytes(size int, offset ...int) ([][]byte, error) {
	ls := [][]byte(nil)
	_, err := this.Limit(size, conv.DefaultInt(0, offset...), func(i int, bs []byte) (any, bool) {
		ls = append(ls, bs)
		return struct{}{}, true
	})
	return ls, err
}

func (this *Scanner) Limit(size int, offset int, search func(i int, bs []byte) (any, bool)) ([]any, error) {

	if search == nil {
		search = func(i int, bs []byte) (any, bool) {
			return bs, true
		}
	}

	var result []any
	for index := 0; this.Scanner.Scan(); index++ {

		//数据筛选,通过[]byte()重新声明内存,否则会复用scanner.token,造成数据混乱
		v, ok := search(index, []byte(this.Scanner.Text()))
		if !ok {
			continue
		}

		//进行分页
		if index < offset {
			continue
		}

		switch {
		case size < 0:
			result = append(result, v)
		case len(result) < size:
			result = append(result, v)
			if len(result) == size {
				return result, nil
			}
		default:
			return result, nil
		}
	}

	return result, nil
}
