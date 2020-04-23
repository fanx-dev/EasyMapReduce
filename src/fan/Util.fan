//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

mixin StrUtil {

  static Str[] split(Str str, Str sp, Int max := Int.maxVal) {
    if (sp.size == 0) {
      return [str]
    }
    res := Str[,]
    while (true) {
      if (res.size == max-1) {
        res.add(str)
        break
      }
      i := str.index(sp)
      if (i == null) {
        res.add(str)
        break
      }

      part := str[0..<i]
      res.add(part)

      start := i + sp.size
      if (start < str.size) {
        str = str[start..-1]
      } else {
        str = ""
      }
    }

    return res
  }

  static Str? extractPart(Str str, Str? begin, Str? end) {
    s := 0
    if (begin != null) {
      p0 := str.index(begin)
      if (p0 == null) {
        return null
      }
      s = p0 + begin.size
    }

    e := str.size
    if (end != null) {
      p0 := str.index(end, s)
      if (p0 == null) {
        return null
      }
      e = p0
    }
    return str[s..<e]
  }
}

