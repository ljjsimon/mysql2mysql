我自己用的工具，一般不更新。

基于[binlog2sql](https://github.com/danfengcao/binlog2sql) 从binlog解析出sql，然后同步到另一台mysql。

需要修改的配置在 worker.py 中作为全局变量。

```python
python worker.py
```