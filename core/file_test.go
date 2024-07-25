package core

import (
	"os"
	"testing"
)

func TestFile_Del(t *testing.T) {

	file, err := os.Create("./testdata/test.txt")
	if err != nil {
		t.Error(err)
		return
	}
	file.WriteString(`1hhhhh
小米
删除这行
小明
大熊猫
可口可乐
娃哈哈
`)
	file.Close()

	f := File{
		Filename: "./testdata/test.txt",
	}

	if err := f.Del(2); err != nil {
		t.Error(err)
		return
	}

	err = f.Insert(4, []byte("我是第5行"))
	if err != nil {
		t.Error(err)
		return
	}

	f.Insert(5, []byte("测试换行符\n数据"))
	f.Append([]byte("测试换行符\n数据"))
	f.Append([]byte("我是倒数第二行"))
	f.Append([]byte("我是倒数第一行"))

	t.Log(f.Limit(func(i int, bs []byte) (any, bool) {
		return string(bs), true
	}, 6, 1))

}
