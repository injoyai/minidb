package minidb

import (
	"testing"
)

func TestNew(t *testing.T) {
	db := New("./database", "test")

	t.Log("Sync")
	if err := db.Sync(new(Person)); err != nil {
		t.Error(err)
		return
	}

	t.Log("Insert")
	if err := db.Insert(&Person{
		Name: "小米",
		Age:  18,
		High: 180.2,
		Boy:  true,
	}); err != nil {
		t.Error(err)
		return
	}
	if err := db.Insert(
		Person{
			Name: "小红",
			Age:  18,
			High: 180.2,
			Boy:  true,
		},
		map[string]interface{}{
			"name": "小白",
			"age":  16,
		},
	); err != nil {
		t.Error(err)
		return
	}

	t.Log("Delete")
	err := db.Where("time=0").Delete(new(Person))
	if err != nil {
		t.Error(err)
		return
	}

	t.Log("FindAndCount")
	list := []*Person(nil)
	co, err := db.Where("name=? and age>=?", "小米", 18).FindAndCount(&list)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("总数量: ", co)
	for _, v := range list {
		t.Logf("%#v", v)
	}

}

func TestDel(t *testing.T) {
	db := New("./database", "test")
	t.Log("Delete")
	err := db.Where("time<=1721890649352277600").Delete(new(Person))
	if err != nil {
		t.Error(err)
		return
	}
}

func TestUpdate(t *testing.T) {
	db := New("./database", "test",
		WithTag("orm"),
	)
	t.Log("Update")
	err := db.Where("time=1721890686324003000").Cols("id,name,age").Update(&Person{
		ID:   666,
		Name: "小白4",
		Age:  22,
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestID(t *testing.T) {
	db := New("./database", "testid",
		WithID("ID"),
	)
	db.Sync(new(Person))
	db.Insert(Person{
		ID:   101,
		Name: "测试",
		Age:  19,
		High: 181.20,
		Boy:  false,
	})
	ls := []*Person(nil)
	db.FindAndCount(&ls)
	for _, v := range ls {
		t.Logf("%#v", v)
	}
}

func TestSplit(t *testing.T) {
	db := New("./database", "testsplit",
		WithSplit([]byte(" # ")),
	)
	db.Sync(new(Person))
	db.Insert(Person{
		ID:   101,
		Name: "测试",
		Age:  19,
		High: 181.20,
		Boy:  false,
	})
	ls := []*Person(nil)
	db.FindAndCount(&ls)
	for _, v := range ls {
		t.Logf("%#v", v)
	}
}

// TestInsert 测试插入数据库后,主键是否被赋值
func TestInsert(t *testing.T) {
	db := New("./database", "testinsert")
	db.Sync(new(Person))
	p := &Person{
		ID:   0,
		Name: "小黑",
		Age:  11,
		High: 132.1,
		Boy:  false,
	}
	t.Log(db.Insert(p))
	t.Log(*p)
}

func TestErr(t *testing.T) {
	db := New("./database", "testerr")
	t.Log(db.Insert(nil))
	t.Log(db.Insert(new(Person)))
}

type Person struct {
	ID   int     `orm:"time"`
	Name string  `orm:"name"`
	Age  int     `orm:"age"`
	High float64 `orm:"high"`
	Boy  bool    `orm:"boy"`
}

func TestCount(t *testing.T) {
	db := New("./database", "testcount")
	db.Sync(new(Person))
	co, err := db.Count(&Person{})
	if err != nil {
		t.Error(err)
		return
	}
	if co == 0 {
		db.Insert(&Person{Name: "A"}, &Person{Name: "B"}, &Person{Name: "C"}, &Person{Name: "D"})
	}
	data := []*Person(nil)
	co, err = db.Where("name=A").Limit(2).FindAndCount(&data)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(data)
	t.Log(co)
}
