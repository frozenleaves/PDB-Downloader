### PDB数据库全库下载工具

### 用法

下载仓库内的pdb_index.html文件，传入代码中替换HTML_FILE_PATH，然后根据实际情况修改保存目录，最大连接数等参数，配置完成后使用`python pdb_downloader.py`即可启动下载。

注：仓库内的pdb_index.html文件可能较老，无法包含pdb数据库中新上传的文件，该索引文件截止日期为2025-08-01，如需最新索引，请下载官方页面：[https://files.pdbj.org/pub/pdb/data/structures/all/pdb/](https://files.pdbj.org/pub/pdb/data/structures/all/pdb/)

特别声明：
**截止到2025-08-01，PDB全库大约有70万个文件，全库下载耗时非常长，请酌情使用。另外，为了减轻数据库服务器的压力，请勿将并发数量设置过高。**
