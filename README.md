# note-sharing

创建数据库
CREATE DATABASE ebook_platform CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'ebook_admin'@'localhost' IDENTIFIED BY 'ebook_123456';
GRANT ALL PRIVILEGES ON ebook_platform.* TO 'ebook_admin'@'localhost';

gradle加载

确保8080端口没被占用

运行LoginApplication

前端：
npm install

package.json 文件运行server
