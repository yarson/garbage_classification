## 垃圾分类数据正/倒排索引构建
使用spark对四类垃圾分类数据进行索引构建。

正排产出格式：id`\t`chineseName`\t`englishName`\t`garbageType`\t`describe

倒排产出格式：word`\1`docId`:`weight`,`docId`:`weight`,`...

### 启动参数格式
```
inputPath=<path> \
forwardIndexPath=<path> \
invertIndexPath=<path>
```
* inputPath: 垃圾分类数据所在目录
* forwardIndexPath: 正排数据产出路径
* invertIndexPath: 倒排数据产出路径

### TODO
* 索引写入文件前检测是否已存在；
* 定义其他分词及打分方式；