# 垃圾分类
帮助大家轻松的对垃圾进行分类。
建立四类垃圾的开源集合，大家可以一起补充未包含的垃圾，之后(个人或单位等)对四类垃圾数据建立倒排索引，用于实时查询。

### 垃圾四大类
* 有害垃圾
* 可回收垃圾
* 湿垃圾
* 干垃圾

### 数据结构
`中文名(chinese),英文名(english),类型(type),描述(describe)`
* 中文名(chinese): 垃圾的中文名
* 英文名(english): 垃圾的英文名
* 类型(type): 垃圾的类型
* 描述(describe): 解释性描述或其他说明

ex: `蔬菜叶,leaves of vegetable,湿垃圾`

#### 有害垃圾
存储于[hazardous.txt](./src/data/hazardous.txt)

#### 可回收垃圾
存储于[recyclable.txt](./src/data/recyclable.txt)

#### 湿垃圾
存储于[wet.txt](./src/data/wet.txt)

#### 干垃圾
存储于[dry.txt](./src/data/dry.txt)
