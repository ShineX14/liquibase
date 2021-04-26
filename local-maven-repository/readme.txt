以下文件是本地测试时的占位文件，实际运行安装包中的文件。
init-user.xml
recompile-plsql.xml

运行ant打包和发布
* 修改settings.xml增加server/user/password配置，用于下载local-maven-repository
* ant build
* 修改settings.xml删除server/user/password配置，避免将敏感信息一起打包发布
* ant deploy