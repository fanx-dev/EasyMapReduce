//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

mixin Processor {
  abstract Str? call(Str in)
  virtual Str? end() { null }
}

class DefaultProcessor : Processor {
  override Str? call(Str in) {
    try return run(in)
    catch (Err e) { e.traceTo(Env.cur.err) }
    return null
  }
  protected virtual Str? run(Str in) { in }
}

class Mapper : DefaultProcessor {
  |Str->Str?| func

  new make(|Str->Str?| func) {
    this.func = func
  }

  override Str? run(Str in) {
    if (in.isEmpty) return ""
    out := func(in)
    return out
  }
}

class Reducer : DefaultProcessor {
  Obj? reduceInit
  |Obj? env, Str val, Str key->Obj?| func
  |Str key, Obj? env->Str?|? finalOut

  protected Str? currentKey
  protected Obj? reduceEnv

  new make(Obj? reduceInit, |Obj? env, Str val, Str key->Obj?| func, |Str key, Obj? env->Str?|? finalOut := null) {
    this.reduceInit = reduceInit
    this.func = func
    this.finalOut = finalOut
  }

  override Str? end() {
    if (reduceEnv == null) return null
    res := outStr
    reduceEnv = null
    return res
  }

  protected virtual Str? outStr() {
    if (finalOut == null) {
      return "$currentKey\t$reduceEnv"
    } else {
      return finalOut(currentKey, reduceEnv)
    }
  }

  override Str? run(Str in) {
    if (in.isEmpty) return ""

    vs := StrUtil.split(in, "\t", 2)
    Str k := vs.getSafe(0, "")
    Str v := vs.getSafe(1, "")

    //echo("split0 $in:$k,$v")

    Str? result
    if (currentKey == null || currentKey != k) {
      if (reduceEnv != null) {
        result = outStr
      }
      currentKey = k
      if (reduceInit != null && reduceInit is Func) {
        reduceEnv = ((Func)reduceInit).call
      }
      else {
        reduceEnv = reduceInit
      }
    }

    reduceEnv = func(reduceEnv, v, k)
    return result
  }
}

