# dcmha
高可用client端，主要作用 <br /> 
  1、实时监测本机mysql服务状态，起心跳作用 <br /> 
  2、mysql服务宕机服务器未宕机的情况，接受服务端请求发送未同步的数据进行追加 <br /> 
  3、服务器宕机重启对未同步的数据进行回滚，保证数据一致性 <br /> 
  4、mysql宕机恢复之后自动恢复同步到新master
 
 config中有client.conf有一些配置项，除mysql用户密码端口、binlog位置根据mysql的实际情况配置外，zk目录需要和server的对应，socket的端口也一定要和zk中记录的对应，mysql_check_retry为mysql宕机重试次数，一次一秒
