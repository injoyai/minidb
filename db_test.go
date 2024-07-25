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
	db := New("./database", "test")
	t.Log("Update")
	err := db.Where("time=1721890686324003000").Cols("id,name,age").Update(&Person{
		ID:   666,
		Name: "小白2",
		Age:  27,
	})
	if err != nil {
		t.Error(err)
		return
	}
}

type Person struct {
	ID   int     `json:"time"`
	Name string  `json:"name"`
	Age  int     `json:"age"`
	High float64 `json:"high"`
	Boy  bool    `json:"boy"`
}
