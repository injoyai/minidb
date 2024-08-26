# 背景

- 项目上有这个样一个开发板需要软件开发
- 配置是磁盘剩余7MB,总内存为10MB,有点开发单片机的感觉
- 这样的配置使用`SQLite`也是太重了,平时项目使用观察下来能到MB档位
- 第一想法是直接存文件,一个模块(对应表)存一个文件,全部读取出来进行增删改查
- 这样有点琐碎,每次都得来一遍相应的逻辑,不如直接提取出来做一个"单片机数据库"
- 于是就有了这个项目,命名规则参考的`xorm`,暂不支持SQL语句和复杂操作(如or,in)

## 如何使用

- 下载安装

  ```shell
    go get github.com/injoyai/minidb
  ```


- 引用包

  ```go
      package main
    
      import (
          "fmt"
          "githubcom/injoyai/minidb"
      )
  
      type Person struct{
          Name string `orm:"name"`
          Age int `orm:"age"`
      }
    
      func main(){
          db:=minidb.New("./database/","project",
              minidb.WithTag("orm"),//设置解析的tag
              minidb.WithID("time"),//设置主键 
                    ) 
          
          result:=[]*Person(nil)
          count,err:=db.Where("name=?","小米").FindAndCount(&result)
          if err!=nil{
              panic(err)
                }    
          fmt.Println("总数量: ",count)
          for _,v:=range result{
              fmt.Println(*v)
          } 
    
      }
  
  ```

## 技术支持

类型转换库[conv](https://github.com/injoyai/conv)

## 获取更多信息



