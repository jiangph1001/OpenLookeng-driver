# OpenLookeng-Driver



## 简介

python驱动上执行OpenLookengsql命令，基于网页API和REST API

目前仅具备执行sql指令，以及获取执行状态的功能，暂不支持获得查询结果

## 用法

### 连接数据库

```
import openlookeng_driver
client = openlookeng_driver.Client(host='192.168.40.152',port=8080,user='lk',catalog='clickhouse223',schema='ssb')
```

### 基于网页API的执行

当调用此接口时，web端也可以看到此次查询的历史

```
result = client.web_execute(sql)
```

**接口**

- get_result(timeout = 5) 
  - 功能：等待sql语句的执行返回结果
  - 参数：timeout为最长等待时间，超时则返回None
  - 返回：关于执行结果的dict。若执行出错则会直接输出错误原因
- get_used_time(timeout = 5)
  - 功能：返回该sql语句的执行时间，单位秒
- get_infoUri(timeout = 5)
  - 功能：获得sql执行详情页的链接
- get_output(timeout = 5)
  - 功能：直接打印输出结果
  - 返回：无
- get_csv_path(timeout = 5)
  - 功能：获取包含结果的csv的url



## 基于REST API的执行
不推荐使用这个方法，因为不稳定能获取到输出结果
```
result = client.execute(sql)
```

**可调用的方法**

- get_result(timeout = 5) 
  - 功能：等待sql语句的执行返回结果
  - 参数：timeout为最长等待时间，超时则返回None
  - 返回：关于执行结果的dict。若执行出错则会直接输出错误原因
- get_used_time(timeout = 5)
  - 功能：返回该sql语句的执行时间，单位秒
- get_infoUri(timeout = 5)
  - 功能：获得sql执行详情页的链接
- print_result()
  - 功能：输出执行状态及**尝试输出**sql执行查询的返回

