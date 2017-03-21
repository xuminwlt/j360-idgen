## j360-idgen

ID生成工具,提供多种ID生成策略组合的ID生成工具

## ID生成策略

- UUID
- 连续串行
- 混合
- 包名
- 时间戳
- 类名

## 存储依赖

1. PostgreSQL(默认)/Oracle
2. Redis

## 参考

将sjdbc ID生成策略集成进来统一管理 http://dangdangdotcom.github.io/sharding-jdbc/post/id-generator/


## 使用

```
public interface IDPool {
	
    String borrow();

    void giveback(String id);

    void consume(String id);
}
```

