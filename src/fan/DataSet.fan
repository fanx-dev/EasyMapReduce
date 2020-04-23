//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

class DataSet {
  internal Context ctx { private set }
  internal DataSet? parent { private set }
  internal DataSet? joinWith { private set }

  internal Uri[] input := [,] { private set }
  internal Uri[] joinInput := [,] { private set }
  internal Uri[] output := [,] { private set }
  internal Processor? processor { private set }
  internal Bool isMapper := true { private set }

  internal Job? job { internal set }
  internal Int id { private set }

  Int? reduceTaskNum := 100
  Str? compression

  internal Str? sortType { private set }

  override Str toStr() {
    s := isMapper? "m" : "r"
    in := input.map{it.name}
    out := output.map{it.name}
    return "$id,${job!=null},$s,$in=>$out"
  }

  new makeEmpty(Context c, Uri[] input) {
    ctx = c
    id = ctx.stepCount++
    this.processor = null

    this.input = input
    if (this.input.isEmpty) {
      this.input.add(ctx.tempDir + `temp_${id}_in.txt`)
    }
    output = this.input
  }

  new makeFrom(DataSet o, Processor processor, Bool isMapper := true) {
    ctx = o.ctx
    id = ctx.stepCount++
    this.isMapper = isMapper
    this.processor = processor
    parent = o

    input.addAll(parent.output)
    output = [ctx.tempDir + `temp_${id}_out.txt`]
  }

  new makeJoin(DataSet o, DataSet d2, Processor mapper) {
    ctx = o.ctx
    id = ctx.stepCount++
    this.isMapper = true
    this.processor = mapper
    parent = o
    joinWith = d2

    input.addAll(parent.output)
    input.addAll(d2.output)
    joinInput.addAll(d2.output)
    output = [ctx.tempDir + `temp_${id}_out.txt`]
  }

  **
  ** convert input line to key+"\t"+value
  **
  This map(|Str line->Str?| mapper) {
    makeFrom(this, Mapper(mapper), true)
  }

  **
  ** reduce by key
  ** reduceInit: reduceEnv initial value or Func
  ** finalOut: convert reduceEnv to Str at the end of reduce
  **
  This reduce(Obj? reduceInit, |Obj? acc, Str val, Str key->Obj?| reducer, |Str key, Obj? acc->Str?|? finalOut := null) {
    makeFrom(this, Reducer(reduceInit, reducer, finalOut), false)
  }

  This defaultReduce(Int reduceTaskNum) {
    n := makeFrom(this, DefaultProcessor(), false)
    n.reduceTaskNum = reduceTaskNum
    return n
  }

  This join(DataSet right, JoinType joinType := JoinType.left) {
    ds := makeJoin(this, right, JoinMapper())
    return makeFrom(ds, JoinReducer(joinType), false)
  }

  Void save(Uri out) {
    if (this.processor == null) {
      throw Err("unsupport save empty")
    }
    output = [out]
    ctx.submit(this)
  }

  This sort(|Str line->Obj?| score, Str sortType := "nr") {
    mapper := makeFrom(this, SortMapper(score), true)
    mapper.sortType = "-k1" + sortType
    rd := makeFrom(mapper, SortReducer(), false)
    rd.reduceTaskNum = 1
    return rd
  }

  ** count line
  Int count() {
    mapper := makeFrom(this, CountMapper(), true)
    rd := makeFrom(mapper, CountReducer(), false)
    rd.reduceTaskNum = 1
    res := rd.collect
    if (res == null) return 0
    return res.trim.toInt
  }

  ** load to memory
  Str? collect() {
    ctx.submit(this)
    if (!ctx.isClient) return null

    buf := Buf()
    Str[]? cmd
    outPath := output.first.toStr
    if (ctx.isDebug) {
        cmd = ["cat", outPath]
    }
    else {
        HADOOP_HOME := Env.cur.vars.get("HADOOP_HOME")
        if (HADOOP_HOME == null) {
            HADOOP_HOME = "/usr/bin/hadoop/software/hadoop"
        }
        hadoop := "$HADOOP_HOME/bin/hadoop"
        cmd = [hadoop, "fs", "-cat", outPath+"/*"]
    }

    echo(cmd)

    p := Process(cmd)
    p.out = buf.out
    p.run.join

    while (buf.size == 0) {
        p.join
    }
    buf.flip
    return buf.readAllStr
  }
}
