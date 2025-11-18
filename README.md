# note-sharing
Login_api_V2 已经实现用户信息管理和笔记管理

1、打开mysql

2、下载minio，将用户名和密码分别设为：name  和   password
并创建名为notesharing的bucket
minio配置教程：https://blog.csdn.net/weixin_55049722/article/details/143193778

3、创建数据库
CREATE DATABASE ebook_platform CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'ebook_admin'@'localhost' IDENTIFIED BY 'ebook_123456';
GRANT ALL PRIVILEGES ON ebook_platform.* TO 'ebook_admin'@'localhost';
使用命令`.\minio.exe server "your\minio\address\bin" --console-address "127.0.0.1:9005" --address "127.0.0.1:9000"`

4、使用IDEA，gradle自动配置

5、执行一次LoginApplication.java，自动配置完后并关闭（确保8080端口没被占用）

6、将/src/main/resources/db.migration中所有sql文件，复制粘贴到mysql中在ebook_platform数据库下执行

7、重新执行LoginApplication.java

后面再次执行后端时只需执行LoginApplication.java

注意：
前后端一起执行时，先让后端跑起来，再执行前端（确保8080端口属于后端）

前端在根据后端写前端时，可以打开http://localhost:8080/swagger-ui/inde.html,在里面查看请求格式和相应的后端返回格式
