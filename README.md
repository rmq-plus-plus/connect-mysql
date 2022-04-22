### connect-mysql
可以接受mysql binlog，发送到指定的http服务

### 使用
- 准备
  - 支持cloud event协议的http服务接口
  - 支持读取binlog的mysql账号密码
  - binlog的 GTID或者位点信息。source将从这个位点开始接受binlog
  
- 生成配置json字符串。通过 main_input_test.go中的TestBuildMainInput()测试方法，生成json配置字符串并且转义。
- 编译main
- 执行./main json配置， 查看http服务是否接受到binlog

### 测试
test目录中app.py可以启动一个http服务， 可以配置这个服务本地测试