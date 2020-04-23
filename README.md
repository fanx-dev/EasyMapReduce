# EasyMapReduce

Write Hadoop MapReduce in high-level API. Inspired by Apache Spark

# Word Count
```
  Void main() {
    init
    ds := c.load(input).map{ it.split(' ').join("\n") }.reduce(0)|Int r,v|{ r + 1 }
    ds.save(output)
  }
```

#### Run

```
cd demo
fan wordCount.fan
```

#### Config
Config fantom runtime to upload.
fantom/etc/easyMapReduce/config.props:
```
fanHome=/your_path/fantom
```

#### Debug
Running in a local simulation env.
```
fan wordCount.fan -debug
```

#### How it works
Show more detail by '-debug_show' args.
```
fan wordCount.fan -debug_show
```

#### Passing Hadoop Args
```
fan wordCount.fan -hadoop '-files ./dict,./data'
```
More [Hadoop Streaming Args](https://hadoop.apache.org/docs/r1.2.1/streaming.html)

