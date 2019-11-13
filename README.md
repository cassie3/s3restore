# s3restore

ECS 对象恢复工具使用说明

该工具可以将不存在的object最近一次的DeleteMarker的信息删除从而实现物件恢复的目的。

前置条件： 需要恢复的文件所属的bucket已经开启version的设置， 可以track到文件的version信息。

##### 帮助文档

```shell
usage: s3restore [-h] --user USER --password PASSWORD --bucket BUCKET
                 [--key KEY] [--file FILE_NAME] --endpoint SERVER
                 [--prefix PREFIX]

restore objects from file

optional arguments:
  -h, --help           show this help message and exit
  --user USER          s3 user
  --password PASSWORD  password for special s3 user
  --bucket BUCKET      bucket name
  --key KEY            retore key name
  --file FILE_NAME     the file which restore files name
  --endpoint SERVER    The address of server
  --prefix PREFIX      the prefix of object
```

##### 使用示例

恢复bucket下面的指定对象 file2/test1

```shell
 ./s3restore --endpoint http://172.16.3.98:9020 --user object_user1 --password ChangeMeChangeMeChangeMeChangeMeChangeMe --bucket test1 --key file2/test1
```

恢复bucket下面的指定以file2/test1为前缀的对象

```shell
./s3restore --endpoint http://172.16.3.98:9020 --user object_user1 --password ChangeMeChangeMeChangeMeChangeMeChangeMe --bucket test1 --prefix file2/test1
```

恢复bucket下指定文件内记录的所有对象

```shell
./s3restore --endpoint http://172.16.3.98:9020 --user object_user1 --password ChangeMeChangeMeChangeMeChangeMeChangeMe --bucket test1 --file file_list
```

