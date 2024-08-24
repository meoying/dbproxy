

## 全局预定义datasources

数据源表示方式:



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
  # 模版写法展开后,等效下方[cn_test,hk_test,cn_prod,hk_prod]
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

```yaml
# 上方

rules:
  order:
    datasources:
      ref:
        - datasources.cn_test
        - datasources.cn_prod
        - datasources.hk_test
        - datasources.hk_prod
```

等效于下方,在局部datasources直接声明

```yaml
rules:
  order:
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

待确认:

1. 是否支持如下语法, 局部datasources声明中既有模版语法又要有其他变量,那么此时模版类型需要给变量名,这与上面匿名模版类型不一致.
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