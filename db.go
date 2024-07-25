package minidb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/injoyai/conv"
	"github.com/injoyai/logs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

func New(dir, database string) *DB {
	os.MkdirAll(filepath.Join(dir, database), os.ModePerm)
	return &DB{
		Dir:      dir,
		Database: database,
		Split:    []byte{' ', 0xFF, ' '},
		tag:      "json",
		id:       "time",
	}
}

/*
DB
前12行预留,表信息
第1-3行预留:	表信息,配置
第4行字段: 	ID , Name , Age , High , boy
第5行类型: 	int , string , int , float , bool
第6行序号: 	1 , 2 , 3 , 4 , 5
第7行备注: 	主键 , 名称 , 年龄 , 身高 , 男
第13行值: 	1 , 小明 , 18 , 180.2 , true
*/
type DB struct {
	Dir      string
	Database string
	Split    []byte
	tag      string
	id       string
	lastID   int64
	mu       sync.Mutex
	table    *Table //表信息
}

// Sync 同步表信息到数据库
func (this *DB) Sync(tables ...interface{}) error {
	for _, table := range tables {

		tableName, err := this.tableName(table)
		if err != nil {
			return err
		}

		fields := Fields{
			{
				Name: this.id,
				Type: Int,
				Memo: "主键,时间戳",
			},
		}
		t := reflect.TypeOf(table)
		if t.Kind() != reflect.Ptr {
			return fmt.Errorf("必须为指针类型: %T", table)
		}
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i).Tag.Get(this.tag)
				if len(field) == 0 {
					field = t.Field(i).Name
				}
				if field == fields[0].Name {
					//忽略掉time字段,该字段为默认主键
					continue
				}
				fields = append(fields, &Field{
					Index: i + 1,
					Name:  field,
					Type:  this.typeString(t.Field(i).Type.Kind()),
				})
			}

		case reflect.Map:
			for i, k := range reflect.ValueOf(table).MapKeys() {
				fields = append(fields, &Field{
					Index: i + 1,
					Name:  k.String(),
					Type:  this.typeString(k.Kind()),
				})
			}

		default:
			return fmt.Errorf("未知类型: %T", table)
		}

		filename := this.filename(tableName)
		//生成表(文件)
		//判断文件是否存在,不存在则新建及初始化
		_, err = os.Stat(filename)
		if err != nil && !os.IsNotExist(err) {
			return err
		} else if err == nil {
			//todo 后续加入同步字段
			return nil
		}

		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			return err
		}

		lsName, lsType, lsMemo := fields.List()
		f.Write([]byte("start\n")) //第1行起始标识
		f.Write([]byte("\n"))      //第2行预留
		f.Write([]byte("\n"))      //第3行预留
		f.Write([]byte(strings.Join(lsName, string(this.Split)) + "\n"))
		f.Write([]byte(strings.Join(lsType, string(this.Split)) + "\n"))
		f.Write([]byte(strings.Join(make([]string, len(lsName)), string(this.Split)) + "\n"))
		f.Write([]byte(strings.Join(lsMemo, string(this.Split)) + "\n"))
		f.Write([]byte("\n"))    //第8行预留
		f.Write([]byte("\n"))    //第9行预留
		f.Write([]byte("\n"))    //第10行预留
		f.Write([]byte("\n"))    //第11行预留
		f.Write([]byte("end\n")) //第12行结束标识
		f.Close()
	}
	return nil
}

// Where Where("Name=?","小明")
func (this *DB) Where(s string, args ...interface{}) *Action {
	return NewAction(this).Where(s, args...)
}

func (this *DB) Insert(i ...interface{}) error {
	return NewAction(this).Insert(i...)
}

func (this *DB) Get(i interface{}) (bool, error) {
	return NewAction(this).Get(i)
}

func (this *DB) Find(i interface{}) error {
	return NewAction(this).Find(i)
}

func (this *DB) Count() (int64, error) {
	return NewAction(this).Count()
}

func (this *DB) FindAndCount(i interface{}) (int64, error) {
	return NewAction(this).FindAndCount(i)
}

/*



 */

func (this *DB) typeString(Type reflect.Kind) string {
	switch Type {
	case reflect.Bool:
		return Bool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Int
	case reflect.Float64, reflect.Float32:
		return Float
	case reflect.String, reflect.Slice:
		return String
	default:
		return String
	}
}

func (this *DB) tableName(table interface{}) (string, error) {
	switch val := table.(type) {
	case nil:
		return "", errors.New("不能为nil")
	case string:
		return val, nil
	case interface{ TableName() string }:
		return val.TableName(), nil
	default:
		t := reflect.TypeOf(table)
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Slice:
			t = t.Elem()
			for t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			return t.Name(), nil
		}
		return t.Name(), nil
	}
}

func (this *DB) filename(tableName string) string {
	return filepath.Join(this.Dir, this.Database, tableName+".mini")
}

// getID 修改时间可能会有问题,主键变小了,后续改成记录最后的id,然后增加
func (this *DB) getID() int64 {
	this.mu.Lock()
	defer this.mu.Unlock()
	for {
		id := time.Now().UnixNano()
		if id > this.lastID {
			this.lastID = id
			return id
		}
	}
}

type Field struct {
	Index int    //实际下标
	Name  string //名称
	Type  string //类型
	Memo  string //备注
	Value string //值
	Sort  int    //排序 序号
}

func (this *Field) compare(Type string, value interface{}) bool {
	if this == nil {
		return false
	}
	switch Type {
	case "like", " like ":
		return strings.Contains(this.Value, conv.String(value))
	case "=":
		switch this.Type {
		case Int:
			return conv.Int(this.Value) == conv.Int(value)
		case Float:
			return conv.Float64(this.Value) == conv.Float64(value)
		default:
			return this.Value == conv.String(value)
		}
	case ">":
		switch this.Type {
		case Int:
			return conv.Int(this.Value) > conv.Int(value)
		case Float:
			return conv.Float64(this.Value) > conv.Float64(value)
		default:
			return this.Value > conv.String(value)
		}
	case ">=":
		switch this.Type {
		case Int:
			return conv.Int(this.Value) >= conv.Int(value)
		case Float:
			return conv.Float64(this.Value) >= conv.Float64(value)
		default:
			return this.Value >= conv.String(value)
		}
	case "<":
		switch this.Type {
		case Int:
			return conv.Int(this.Value) < conv.Int(value)
		case Float:
			return conv.Float64(this.Value) < conv.Float64(value)
		default:
			return this.Value < conv.String(value)
		}
	case "<=":
		switch this.Type {
		case Int:
			return conv.Int(this.Value) <= conv.Int(value)
		case Float:
			return conv.Float64(this.Value) <= conv.Float64(value)
		default:
			return this.Value <= conv.String(value)
		}
	default:
		logs.Err("未知的比较类型: ", Type)
		return false
	}
}

type Fields []*Field

func (this Fields) List() ([]string, []string, []string) {
	lsName := []string(nil)
	lsType := []string(nil)
	lsMemo := []string(nil)
	for _, f := range this {
		lsName = append(lsName, f.Name)
		lsType = append(lsType, f.Type) // conv.String(f.Type)
		lsMemo = append(lsMemo, f.Memo)
	}
	return lsName, lsType, lsMemo
}

func (this Fields) Map() map[string]*Field {
	m := make(map[string]*Field)
	for _, f := range this {
		m[f.Name] = f
	}
	return m
}

func (this Fields) MapIndex() map[int]*Field {
	m := make(map[int]*Field)
	for _, f := range this {
		m[f.Index] = f
	}
	return m
}

func (this *DB) DecodeTable(ls [][]byte) (*Table, error) {
	if len(ls) != 12 {
		return nil, errors.New("无效文件")
	}
	t := new(Table)
	for i, bs := range ls {
		switch i {
		case 0:
			if string(bs) != "start" {
				return nil, errors.New("文件格式不正确.start")
			}
		case 1, 2:
			//预留,编码,等配置信息
		case 3:
			//字段名称
			for index, item := range bytes.Split(bs, this.Split) {
				t.Fields = append(t.Fields, &Field{
					Index: index,
					Name:  string(item),
					Type:  String,
					Memo:  "",
				})
			}
		case 4:
			//字段类型
			for index, item := range bytes.Split(bs, this.Split) {
				if index < len(t.Fields) {
					t.Fields[index].Type = string(item)
				}
			}
		case 5:
			//字段序号
			for index, item := range bytes.Split(bs, this.Split) {
				if index < len(t.Fields) {
					t.Fields[index].Sort = conv.Int(string(item))
				}
			}
		case 6:
			//字段备注
			for index, item := range bytes.Split(bs, this.Split) {
				if index < len(t.Fields) {
					t.Fields[index].Memo = string(item)
				}
			}
		case 7, 8, 9, 10:
			//预留配置
		case 11:
			if string(bs) != "end" {
				return nil, errors.New("文件格式不正确.end")
			}
		}
	}
	return t, nil
}

type Table struct {
	Name   string //表名
	Fields Fields //字段信息
}

//func (this *Table) Bytes() []byte {
//	f := bytes.NewBuffer(nil)
//	f.Write([]byte("start\n")) //第1行起始标识
//	f.Write([]byte("\n"))      //第2行预留
//	f.Write([]byte("\n"))      //第3行预留
//	f.Write([]byte(strings.Join(lsName, string(this.Split)) + "\n"))
//	f.Write([]byte(strings.Join(lsType, string(this.Split)) + "\n"))
//	f.Write([]byte(strings.Join(make([]string, len(lsName)), string(this.Split)) + "\n"))
//	f.Write([]byte(strings.Join(lsMemo, string(this.Split)) + "\n"))
//	f.Write([]byte("\n"))    //第8行预留
//	f.Write([]byte("\n"))    //第9行预留
//	f.Write([]byte("\n"))    //第10行预留
//	f.Write([]byte("\n"))    //第11行预留
//	f.Write([]byte("end\n")) //第12行结束标识
//	return f.Bytes()
//}

func (this *Table) DecodeData2(data []byte, split []byte) map[string]*Field {
	mFieldIndex := this.Fields.MapIndex()
	//数据整理
	mapField := make(map[string]*Field)
	for i, bs := range bytes.Split(data, split) {
		if field, ok := mFieldIndex[i]; ok {
			//todo 根据类型转成对应的格式
			mapField[field.Name] = &Field{
				Index: field.Index,
				Name:  field.Name,
				Type:  field.Type,
				Memo:  field.Memo,
				Value: string(bs),
				Sort:  field.Sort,
			}
		}
	}
	return mapField
}

func (this *Table) DecodeData(s *bufio.Scanner, split []byte, fn func(index int, field map[string]*Field) bool) {
	mFieldIndex := this.Fields.MapIndex()
	for index := 0; s.Scan(); index++ {
		//数据整理
		mapField := make(map[string]*Field)
		for i, bs := range bytes.Split(s.Bytes(), split) {
			if field, ok := mFieldIndex[i]; ok {
				//todo 根据类型转成对应的格式
				mapField[field.Name] = &Field{
					Index: field.Index,
					Name:  field.Name,
					Type:  field.Type,
					Memo:  field.Memo,
					Value: string(bs),
					Sort:  field.Sort,
				}
			}
		}
		if !fn(index, mapField) {
			return
		}
	}
}

func (this *Table) EncodeData(field map[string]interface{}, split []byte) []byte {
	mField := this.Fields.Map()
	ls := make([][]byte, len(mField))
	for k, v := range field {
		if f, ok := mField[k]; ok {
			ls[f.Index] = []byte(conv.String(v))
		}
	}
	bs := bytes.Join(ls, split)
	return bs
}

type ValueType string

const (
	String = "string"
	Bool   = "bool"
	Int    = "int"
	Float  = "float"
)
