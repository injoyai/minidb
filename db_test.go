package minidb

import (
	"testing"
)

func TestNew(t *testing.T) {
	db := New("./database", "test")
	if err := db.Sync(new(Person)); err != nil {
		t.Error(err)
		return
	}

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

	list := []*Person(nil)
	co, err := db.FindAndCount(&list)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("总数量: ", co)
	for _, v := range list {
		t.Logf("%#v", v)
	}

}

type Person struct {
	ID   int     `json:"time"`
	Name string  `json:"name"`
	Age  int     `json:"age"`
	High float64 `json:"high"`
	Boy  bool    `json:"boy"`
}
