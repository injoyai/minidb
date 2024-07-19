package minidb

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/injoyai/conv"
	"os"
	"reflect"
	"strings"
)

func NewAction(db *DB) *Action {
	return &Action{db: db}
}

/*
Action
暂不支持or
*/
type Action struct {
	db           *DB
	TableName    string
	Decode       bool
	Handler      []func(field map[string]string) (next bool)
	LimitHandler func(index int, field map[string]string) (done bool)
	Result       []interface{}
	Err          error
}

func (this *Action) Table(table interface{}) *Action {
	switch val := table.(type) {
	case nil:
	case string:
		this.TableName = val
	case interface{ TableName() string }:
		this.TableName = val.TableName()
	default:
		t := reflect.TypeOf(table)
		this.TableName = t.Name()
	}
	return this
}

func (this *Action) Where(s string, args ...interface{}) *Action {
	s = strings.ReplaceAll(s, "?", "%s")
	s = fmt.Sprintf(s, args...)

	strings.Split(s, " and ")

	//分割and
	//然后分割=
	//然后生成函数

	return this
}

func (this *Action) And(s string, args ...interface{}) *Action {
	return this.Where(s, args...)
}

func (this *Action) Cols(cols ...string) *Action {
	m := make(map[string]bool)
	for _, s := range cols {
		for _, v := range strings.Split(s, ",") {
			m[v] = true
		}
	}
	this.Handler = append(this.Handler, func(field map[string]string) (next bool) {
		for k, _ := range field {
			if !m[k] {
				delete(field, k)
			}
		}
		return true
	})
	return this
}

func (this *Action) Limit(size int, offset ...int) *Action {
	this.LimitHandler = func(index int, field map[string]string) bool {
		if len(offset) > 0 && index < offset[0] {
			return false
		}
		this.Result = append(this.Result, field)
		return len(this.Result) >= size
	}
	return this
}

func (this *Action) Get(i interface{}) (bool, error) {
	this.Limit(1)
	if len(this.Result) == 0 {
		return false, nil
	}
	err := conv.Unmarshal(this.Result, i)
	return true, err
}

func (this *Action) Find(i interface{}) error {
	if err := this.setTable(i); err != nil {
		return err
	}
	if err := this.find(); err != nil {
		return err
	}
	return conv.Unmarshal(this.Result, i)
}

func (this *Action) Count() (int64, error) {
	return this.count()
}

func (this *Action) FindAndCount(i interface{}) (int64, error) {
	if err := this.setTable(i); err != nil {
		return 0, err
	}
	co, err := this.count()
	if err != nil {
		return 0, err
	}
	if err = this.find(); err != nil {
		return 0, err
	}
	return co, conv.Unmarshal(this.Result, i)
}

// Insert 插入到数据库
func (this *Action) Insert(i ...interface{}) error {
	for index := 0; index < len(i) && len(this.TableName) == 0; index++ {
		if err := this.setTable(i[index]); err != nil {
			return err
		}
	}
	maps := []map[string]interface{}(nil)
	for _, v := range i {
		for _, vv := range conv.Interfaces(v) {
			m := make(map[string]interface{})
			if err := conv.Unmarshal(vv, &m); err != nil {
				return err
			}
			maps = append(maps, m)
		}
	}
	if len(maps) == 0 {
		return nil
	}
	return this.withAppend(maps...)
}

func (this *Action) Delete() error {
	//this.Cols("ID")
	//result := make([]map[string]string, len(this.Result))
	//for _, v := range this.Result {
	//
	//}
	return nil
}

/*



 */

func (this *Action) setTable(i interface{}) error {
	tableName, err := this.db.tableName(i)
	if err != nil {
		return err
	}
	this.TableName = tableName
	return nil
}

func (this *Action) withAppend(fields ...map[string]interface{}) error {
	filename := this.db.filename(this.TableName)
	f, err := os.OpenFile(filename, os.O_APPEND, 0o666)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("表不存在")
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	table, err := this.getTable(scanner)
	if err != nil {
		return err
	}

	for _, field := range fields {
		field[table.Fields[0].Name] = []byte(conv.String(this.db.getID()))
		if _, err = f.Write(table.DataBytes(field, this.db.Split)); err != nil {
			return err
		}
	}
	return nil
}

func (this *Action) withRead(fn func(f *os.File) error) error {
	filename := this.db.filename(this.TableName)
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("表不存在")
		}
		return err
	}
	defer f.Close()
	if fn != nil {
		return fn(f)
	}
	return nil
}

func (this *Action) getTable(scanner *bufio.Scanner) (*Table, error) {
	infoList := [12][]byte{}
	for i := 0; i < 12; i++ {
		if !scanner.Scan() {
			break
		}
		infoList[i] = scanner.Bytes()
	}
	return this.db.DecodeTable(infoList)
}

func (this *Action) withData(scanner *bufio.Scanner, fn func(t *Table, s *bufio.Scanner) error) error {
	infoList := [12][]byte{}
	for i := 0; i < 12; i++ {
		if !scanner.Scan() {
			break
		}
		infoList[i] = scanner.Bytes()
	}
	table, err := this.db.DecodeTable(infoList)
	if err != nil {
		return err
	}
	return fn(table, scanner)
}

func (this *Action) find() error {
	return this.withRead(func(f *os.File) error {
		return this.withData(bufio.NewScanner(f), func(t *Table, scanner *bufio.Scanner) error {
			t.DecodeData(scanner, this.db.Split, func(index int, field map[string]string) bool {
				//数据筛选
				for _, fn := range this.Handler {
					if !fn(field) {
						break
					}
				}
				//数据分页
				if this.LimitHandler == nil {
					this.LimitHandler = func(index int, field map[string]string) bool {
						this.Result = append(this.Result, field)
						return false
					}
				}
				if this.LimitHandler(index, field) {
					return false
				}

				return true
			})

			return nil
		})
	})
}

func (this *Action) count() (int64, error) {
	count := int64(0)
	err := this.withRead(func(f *os.File) error {
		return this.withData(bufio.NewScanner(f), func(t *Table, s *bufio.Scanner) error {
			for i := 0; s.Scan(); i++ {
				count++
			}
			return s.Err()
		})
	})
	return count, err
}
