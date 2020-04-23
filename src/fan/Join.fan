//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

class JoinMapper : DefaultProcessor {
  private Uri? currentFile
  private Uri[]? joinInput
  private Bool isRight := false

  Void init(Uri? curFile, Uri[]? joinInput) {
    this.currentFile = curFile
    this.joinInput = joinInput
    isRight = false
    if (currentFile != null) {
      for(i:=0; i<joinInput.size; ++i) {
        join := joinInput[i]
        if (currentFile.toStr.contains(join.toStr)) {
          isRight = true
          break
        }
      }
    }
  }

  override Str? run(Str in) {
    if (in.isEmpty) return ""
    vs := StrUtil.split(in, "\t", 2)
    k := vs.getSafe(0, "")
    v := vs.getSafe(1, "")

    Str out := ""
    if (isRight) {
      out = "$k\tr\t$v"
    } else {
      out = "$k\tl\t$v"
    }
    return out
  }
}

enum class JoinType {
  left, in, full
}

class JoinReducer : Reducer {
  JoinType joinType := JoinType.left

  //simple in memory join
  private Str joinList(Str[] leftS, Str[] rightS) {
    if (leftS.isEmpty && rightS.isEmpty) return ""
    sb := StrBuf()

    if (leftS.isEmpty) {
      if (rightS.isEmpty) {
        if (joinType == JoinType.full) {
          return "$currentKey\t\t"
        } else {
          return ""
        }
      }
      else {
        if (joinType == JoinType.full) {
          rightS.each |right| {
            if (sb.size > 0) sb.add("\n")
            sb.add("$currentKey\t\t$right")
          }
          return sb.toStr
        } else {
          return ""
        }
      }
    }

    //echo("$leftS=>$rightS")
    leftS.each |left| {
      if (rightS.isEmpty) {
        if (joinType != JoinType.in) {
          if (sb.size > 0) sb.add("\n")
          sb.add("$currentKey\t$left\t")
        }
      } else {
        rightS.each |right| {
          if (sb.size > 0) sb.add("\n")
          sb.add("$currentKey\t$left\t$right")
        }
      }
    }
    return sb.toStr
  }

  new make(JoinType joinType) : super.make(null, |Obj? env, Str val->Obj?| {
      vs := StrUtil.split(val, "\t", 2)
      Str side := vs.getSafe(0, "")
      Str v := vs.getSafe(1, "")

      //echo("split: $val:$side,$v")
      if (env == null) {
        env = [[,],[,]]
      }
      Str[][] tab := env
      if (side == "l") {
        tab.first.add(v)
      }
      else if (side == "r") {
        tab.last.add(v)
      }
      return env
    }) {
    this.joinType = joinType
  }

  protected override Str? outStr() {
    Str[][] tab := reduceEnv
    return joinList(tab.first, tab.last)
  }
}

