package minidb

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/injoyai/conv"
	"github.com/injoyai/minidb/core"
	"os"
	"strings"
)

func NewAction(db *DB) *Action {
	return &Action{
		db:      db,
		scanner: core.NewFile("", 0),
	}
}

/*
Action
暂不支持or
*/
type Action struct {
	db *DB

	Handler      []func(field map[string]*Field) (mate bool)          //筛选添加,例where and 暂未实现 or
	LimitHandler func(index int, field map[string]string) (done bool) //对应操作Limit
	SortHandler  func(i, j map[string]*Field) bool                    //对应操作Sort
	Result       []interface{}                                        //对应Find和FindAndCount的数据缓存
	Err          error                                                //操作的错误信息

	TableName string     //要操作的表名
	scanner   *core.File //文件操作
	table     *Table     //要操作的表信息
}

func (this *Action) Table(table interface{}) *Action {
	this.setTable(table)
	return this
}

func (this *Action) Where(s string, args ...interface{}) *Action {
	offset := 0
	for _, v := range strings.Split(s, " and ") {
		typeList := []string{" like ", ">=", ">", "<=", "<", "="}
		for _, Type := range typeList {
			if ls := strings.SplitN(v, Type, 2); len(ls) == 2 {
				key := strings.TrimSpace(ls[0])
				value := strings.TrimSpace(ls[1])
				if value == "?" {
					if len(args) > offset {
						value = conv.String(args[offset])
						offset++
					} else {
						this.Err = fmt.Errorf("缺少参数(%s)", v)
						return this
					}
				}
				this.Handler = append(this.Handler, func(field map[string]*Field) bool {
					val, ok := field[key]
					return ok && val.compare(Type, value)
				})
				break
			}
		}

	}
	return this
}

func (this *Action) And(s string, args ...interface{}) *Action {
	return this.Where(s, args...)
}

func (this *Action) Like(filed, like string) *Action {
	return this.Where(fmt.Sprintf("%s like %s", filed, like))
}

func (this *Action) Cols(cols ...string) *Action {
	m := make(map[string]bool)
	for _, s := range cols {
		for _, v := range strings.Split(s, ",") {
			if len(v) > 0 {
				m[v] = true
			}
		}
	}
	//当调用了Cols而未设置值是,视为无效
	if len(m) == 0 {
		return this
	}
	this.Handler = append(this.Handler, func(field map[string]*Field) (next bool) {
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
		if size == 0 {
			return true
		}
		this.Result = append(this.Result, field)
		return len(this.Result) >= size && size > 0
	}
	return this
}

// Desc 倒序,未实现
func (this *Action) Desc(filed string) *Action {
	this.SortHandler = func(i, j map[string]*Field) bool {
		f1 := i[filed]
		f2 := j[filed]
		if f1 == nil || f2 == nil {
			return false
		}
		return f1.compare(">", f2.Value)
	}
	return this
}

// Asc 正序,取最小的字段,未实现
func (this *Action) Asc(filed string) *Action {
	this.SortHandler = func(i, j map[string]*Field) bool {
		f1 := i[filed]
		f2 := j[filed]
		if f1 == nil || f2 == nil {
			return false
		}
		return f1.compare("<", f2.Value)
	}
	return this
}

func (this *Action) Get(i interface{}) (has bool, err error) {
	defer this.dealErr(&err)
	if err := this.setTable(i); err != nil {
		return false, err
	}
	this.Limit(1)
	if err := this.find(); err != nil {
		return false, err
	}
	if len(this.Result) == 0 {
		return false, nil
	}
	err = this.db.unmarshal(this.Result[0], i)
	return true, err
}

func (this *Action) Find(i interface{}) (err error) {
	defer this.dealErr(&err)
	if err := this.setTable(i); err != nil {
		return err
	}
	if err := this.find(); err != nil {
		return err
	}
	return this.db.unmarshal(this.Result, i)
}

func (this *Action) Count(i ...interface{}) (co int64, err error) {
	defer this.dealErr(&err)
	if err := this.setTable(i...); err != nil {
		return 0, err
	}
	return this.count()
}

func (this *Action) FindAndCount(i interface{}) (co int64, err error) {
	defer this.dealErr(&err)
	//设置表名,数据来源
	if err := this.setTable(i); err != nil {
		return 0, err
	}
	//统计数量
	co, err = this.count()
	if err != nil {
		return 0, err
	}
	//查找数据
	if err = this.find(); err != nil {
		return 0, err
	}
	//解析到用户的对象
	return co, this.db.unmarshal(this.Result, i)
}

// Insert 插入到数据库
func (this *Action) Insert(i ...interface{}) (err error) {
	defer this.dealErr(&err)
	//获取表名称
	if err := this.setTable(i); err != nil {
		return err
	}
	//整理字段结构
	return this.scanner.AppendWith(func(s *core.Scanner) ([][]byte, error) {
		ls := [][]byte(nil)
		for _, v := range i {
			for _, vv := range conv.Interfaces(v) {
				field := make(map[string]interface{})
				if err := this.db.unmarshal(vv, &field); err != nil {
					return nil, err
				}
				//设置自增主键
				field[this.db.id] = this.db.getID()
				//把主键赋值到原先的数据字段中,todo 是否有更好的方式?
				fmt.Println(this.db.unmarshal(field, vv))
				ls = append(ls, this.table.EncodeData(field, this.db.Split))
			}
		}
		return ls, nil
	})
}

func (this *Action) Update(i interface{}) (err error) {
	defer this.dealErr(&err)

	//获取表名称
	if err := this.setTable(i); err != nil {
		return err
	}

	//校验是否忘记增加删除的条件
	if len(this.Handler) == 0 && this.LimitHandler == nil {
		return errors.New("修改是否忘记增加条件")
	}

	//解析数据到map中
	update := make(map[string]interface{})
	if err := this.db.unmarshal(i, &update); err != nil {
		return err
	}

	return this.scanner.Update(func(i int, bs []byte) ([][]byte, error) {

		flied := this.table.DecodeData2(bs, this.db.Split)
		original := make(map[string]string)
		for k, v := range flied {
			original[k] = v.Value
		}
		for _, fn := range this.Handler {
			if !fn(flied) {
				//不符合的数据原路返回
				return [][]byte{bs}, nil
			}
		}

		if this.LimitHandler != nil {
			if this.LimitHandler(i, original) {
				//不符合的数据原路返回
				return [][]byte{bs}, nil
			}
		}

		m := make(map[string]interface{})
		for k, v := range original {
			m[k] = v
		}
		for k, v := range update {
			//主键不能修改
			if k != this.db.id {
				if _, ok := flied[k]; ok {
					m[k] = v
				}
			}
		}

		result := this.table.EncodeData(m, this.db.Split)

		return [][]byte{result}, nil
	})
}

func (this *Action) Delete(i ...any) (err error) {
	defer this.dealErr(&err)

	//获取表名称
	if err := this.setTable(i); err != nil {
		return err
	}

	//校验是否忘记增加删除的条件
	if len(this.Handler) == 0 && this.LimitHandler == nil {
		return errors.New("删除是否忘记增加条件")
	}

	return this.scanner.DelBy(func(i int, bs []byte) (bool, error) {
		del := true
		flied := this.table.DecodeData2(bs, this.db.Split)
		for _, fn := range this.Handler {
			if !fn(flied) {
				del = false
			}
		}
		return del, nil
	})
}

/*



 */

func (this *Action) dealErr(err *error) {
	if err != nil && *err != nil {
		switch {
		case os.IsNotExist(*err):
			//文件不存在,表示数据库的表不存在
			*err = errors.New("表不存在")
		}
	}
}

// setTable 解析表名
func (this *Action) setTable(i ...interface{}) error {
	if this.Err != nil {
		return this.Err
	}

	if len(i) == 0 {
		return nil
	}

	table := i[0]
	ls := conv.Interfaces(table)
	if len(ls) > 0 {
		table = ls[0]
	}

	tableName, err := this.db.tableName(table)
	if err != nil {
		return err
	}

	this.TableName = tableName
	this.scanner.Filename = this.db.filename(this.TableName)
	this.scanner.OnOpen(func(s *core.Scanner) ([][]byte, error) {
		ls, err := s.LimitBytes(12)
		if err != nil {
			return nil, err
		}
		this.table, err = this.db.DecodeTable(ls)
		return ls, err
	})
	return nil
}

func (this *Action) withRead(fn func(f *os.File) error) error {
	filename := this.db.filename(this.TableName)
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.New("表不存在: " + this.TableName)
		}
		return err
	}
	defer f.Close()
	if fn != nil {
		return fn(f)
	}
	return nil
}

func (this *Action) withData(scanner *bufio.Scanner, fn func(t *Table, s *bufio.Scanner) error) error {
	infoList := [12][]byte{}
	for i := 0; i < 12; i++ {
		if !scanner.Scan() {
			break
		}
		infoList[i] = []byte(scanner.Text())
	}
	table, err := this.db.DecodeTable(infoList[:])
	if err != nil {
		return err
	}
	return fn(table, scanner)
}

func (this *Action) find() error {
	return this.withRead(func(f *os.File) error {
		return this.withData(bufio.NewScanner(f), func(t *Table, scanner *bufio.Scanner) error {
			t.DecodeData(scanner, this.db.Split, func(index int, field map[string]*Field) bool {
				//数据筛选
				for _, fn := range this.Handler {
					if !fn(field) {
						return true
					}
				}
				//数据分页
				if this.LimitHandler == nil {
					this.LimitHandler = func(index int, field map[string]string) bool {
						this.Result = append(this.Result, field)
						return false
					}
				}
				m := make(map[string]string)
				for k, v := range field {
					m[k] = v.Value
				}

				if this.LimitHandler(index, m) {
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
