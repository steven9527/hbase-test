1 测试前，需先搭好hbase集群

2 程序里，访问hbase集群，是通过设置 conf.set("hbase.zookeeper.quorum", "node06,node08,node09");来实现的
  注意，这里node06,node08,node09是主机名，为了保证能访问，需提前联系运维，将node06,node08,node09配置在运行本
  程序的服务器（开发时，就是本机）的hosts文件里

3 调试前，先了解hbase最基础的命令。

4 可用xshell或者putty等客户端，ssh到一台hbase服务器上，例如 node06上
  执行 hbase shell进入交互界面,如下是最基础的命令
  list （列出所有表)
  create 'tbl','cf'(创建名为 tbl，列族为 cf的表)
  put 'tbl','111','cf:name','steven'(插入一行，rowkey=111，cf:name为列名，值为steven)
  put 'tbl','111','cf:age','15'(插入一行，rowkey=111，cf:age为列名，值为15，注意，这一行和上一行是同一行)
  get 'tbl','111'(tbl表里按rowkey获取一行)
  scan 'tbl'(类似于select * from tbl)
  truncate 'tbl'(清空tbl表)

5 java里，所有的api，都是对上面这些基础命令的封装，例如，java里插入一条数据，就是构建一个put对象
设置 相应的列名，然后执行插入
同样，要获取一行，那么就是构建一个 get对象，设置他的rowkey值；
     要查询，那么就是构建一个scan兑现，设置前缀，以及startrow，stoprow之类的

6 所以，通过程序来实现对hbase的交互，前提就是要了解上诉的基础命令，然后再构建相应的对象