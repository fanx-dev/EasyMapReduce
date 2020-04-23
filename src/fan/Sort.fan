//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

class SortMapper : DefaultProcessor {
  |Str->Obj?| func

  new make(|Str->Obj?| func) {
    this.func = func
  }

  override Str? run(Str in) {
    out := func(in)
    return "$out\t$in"
  }
}

class SortReducer : DefaultProcessor {
  new make() {
  }

  override Str? run(Str in) {
    vs := StrUtil.split(in, "\t", 2)
    //Str k := vs.getSafe(0, "")
    Str v := vs.getSafe(1, "")
    return v
  }
}

