# 配置详解

config.yaml文件

## datasources

### 全局定义

```yaml
# 预定义的全局datasources,可选
datasources:
  # 仅有主
  master_only:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
  # 一主一从
  master_one_slave:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.toB.mysql.meoying.com:3306)/order?xxx
  # 一主多从
  master_multi_slaves:
    master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      
  # 模版写法
  tmpl:
    template:
      master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
      slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
      placeholders:
        region:
          - hk
          - cn
        role:
          - test
          - prod
          
  # 上方模版语法等效于下方定义
  cn_test:
    master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
  hk_test:
    master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
  cn_prod:
    master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
  hk_prod:
    master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx
```

### 局部定义

局部定义是指在`rules.{name}.datasources`中定义变量,除了支持全局定义中的所有类型,还支持引用类型


```yaml
# 
datasources:
  cn_test:
    master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
  hk_test:
    master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
  cn_prod:
    master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
  hk_prod:
    master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx 
rules:
  order:
    datasources:
      # 引用语法
      ref:
        - datasources.cn_test
        - datasources.cn_prod
        - datasources.hk_test
        - datasources.hk_prod
```

使用引用语法等效于在`rules.{name}.datasources`局部直接声明

```yaml
datasources:
  cn_test:
    master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
  hk_test:
    master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
  cn_prod:
    master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
  hk_prod:
    master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx
      
rules:
  order:
    datasources:
      # 引用语法等下于如下直接定义
      cn_test:
        master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
      hk_test:
        master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
      cn_prod:
        master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
      hk_prod:
        master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx
```

还等效于在局部datasources中直接使用模版写法,注意这里没有变量名直接使用了变量类型??

```yaml
rules:
  order:
    datasources:
      template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
```

TODO:待确认:

- 是否支持如下语法, 局部datasources声明中既有模版语法又要有其他变量,那么此时模版类型需要给变量名,这与上面匿名模版类型不一致.
是否需要增加限制, 只有当局部datasources中只有一个变量的时候且为模版类型,则可以简写为匿名

```yaml
rules:
  order:
    datasources:
      tmpl:
        template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
      cn:
        master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx 
```
- datasourceTemplate模版中master与slaves中的占位符不匹配

```yaml
master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
slaves: webook:webook@tcp(${region}.slave.mysql.meoying.com:3306)/order?xxx
```

## placeholders

### 全局定义

- 只支持字符串、数组、哈希类型, 注意:不支持模版类型

```yaml
placeholders:
  str: this is string
  array:
    - hk
    - cn
  id:
    hash:
      key: user_id
      base: 3
```

### 局部定义

- 在rules.datasources、rules.databases、rules.tables的模版中使用时,支持引用类型

```yaml
placeholders:
  region_cn: cn
  region_hk: hk
  key:
    hash:
      key: user_id
      base: 3
  type:
    - tob
    - toc
      
rules:
  user:
    datasources:
      cn:
        template:
          master: webook:webook@tcp(cn.${type}.master.mysql.meoying.com:3306)/order?xxx
          slaves: webook:webook@tcp(cn.${type}.slave.mysql.meoying.com:3306)/order?xxx
          placeholders:
            type:
              ref:
                - placeholders.type 
    databases:
      cn:
        template:
          expr: user_db_${region}
          placeholders:
            region: 
              ref:
                - cn
                - hk 
    tables:
      cn:
        template:
          expr: user_tbl_${key}
          placeholders:
            key:
              ref:
                - placeholders.key 
```

TODO: 待确认问题 —— 引用类型语义问题, 引用多个时候的类型校验和转换问题

```yaml
placeholders:
  region_cn: cn
  region_hk: hk
  key:
    hash:
      key: user_id
      base: 3
  type:
    - tob
    - toc
      
rules:
  user:
    datasources:
      cn:
        template:
          master: webook:webook@tcp(cn.${type}.master.mysql.meoying.com:3306)/order?xxx
          slaves: webook:webook@tcp(cn.${type}.slave.mysql.meoying.com:3306)/order?xxx
          placeholders:
            type:
              ref:
                - placeholders.type 
    databases:
      cn:
        template:
          expr: user_db_${region}
          placeholders:
              # ***引用的语义问题:  人是知道要将两个字符串转为数组/枚举,但是region1和region2没有类型信息
              # 如果根据被引用变量的类型, 如果瞎写可能出现[]Hash?
              # region2可能也是数组,也可能是字符串
              # 引用语法的语义 —— 类型由被引用变量的值确定, 多个就转换为 []Type数组? 可能会出现 []Hash?
            region1:
              ref:
                - placeholders.region_cn
                - placeholders.region_hk
            region2:
              ref:
                - placeholders.region_cn
    tables:
      cn:
        template:
          expr: user_tbl_${key}
          placeholders:
            key:
              ref:
                - placeholders.key 
```
上例中`region`是[cn,hk] 但是placeholders.region_cn和placeholders.region_hk表示两个字符串
,而期望的是将两个字符串转换为字符串数组,这种转换需要明确给出的,否则`placeholders.key`解释不通

## databases

### 全局定义

必须要有变量名, 变量的值类型,支持字符串、数组、哈希类型, 模版类型

```yaml
databases:
  str: user_db
  array:
    - cn_db
    - hk_db
  # 待确认
  hash_var:
    hash:
      key: user_id
      base: 3
  tmpl_var:
    expr: user_db_${key}
    placeholders:
      key:
        hash:
          key: user_id
          base: 3
```

- 模版类型中的占位符可以引用全局占位符变量,但本身不支持引用变量

```yaml
placeholders:
  key:
    hash:
      key: user_id
      base: 3

databases:
  str: user_db
  # str_ref 非法
  str_ref:
    ref:
      - str
  # tmpl_var 合法 
  tmpl_var:
    expr: user_db_${key}
    placeholders:
      key:
        ref:
          - placeholders.key 
```

TODO: 待确认: 是否不该直接支持哈希类型

```yaml
databases:
  # 待确认, 无法单独表示分库规则,只能于模版组合使用
  hash_var:
    hash:
      key: user_id
      base: 3
```

### 局部定义

#### 待确认

是否需要支持命名和匿名? 还是只是匿名?

匿名案例:

```yaml
rules:
  user:
    # 匿名
    databases: user_db
```

```yaml
rules:
  user:
    # 匿名
    databases:
      template:

```

```yaml
rules:
  user:
    # 命名
    databases: 
        cn: cn_user_db
        hk:
          - hk_user_db_0
          - hk_user_db_1
```

```yaml

```




#### 字符串类型

```yaml

rules:
  user:
    databases: user_db

```

## tables

### 全局定义

### 局部定义

## rules

只能全局定义

TODO:
1. 关键字校验, 现在有些情况使用关键字比如:template当变量名,是不会报错的,有些情况会报错, 需要统一一下
2. 引用语义问题,统一问题. 因为引用语法是一个枚举,内部可以包含多个引用路径,但是这并不意味着将多个被引用的变量就要转换为数组.
   - 需要转换为数组的情况, 比如region: 引用两个字符串
   - 不需要的情况,数据源引用
3. 如何从Config结构体中获取信息,给下游用来创建分片算法和初始化数据源