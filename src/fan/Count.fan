//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

class CountMapper : DefaultProcessor {

  private Int count := 0

  new make() {
  }

  override Str? call(Str in) {
      ++count
      return null
  }

  override Str? end() {
    return "line\t$count"
  }
}

class CountReducer : DefaultProcessor {
  private Int count := 0
  private Int column := 1

  new make(Int column := 1) {
      this.column = column
  }

  override Str? run(Str in) {
    vs := StrUtil.split(in, "\t")
    Str? k := vs.getSafe(column)
    if (k != null)
        count += k.toInt
    return null
  }

  override Str? end() {
    return count.toStr
  }
}

