//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

internal class LocalClientJob : Job {
  new make(Context c, Int id, Job? parent) : super.make(c, id, parent) {
  }

  override Bool run() {
    if (isDone) return true
    parent?.run

    if (mapSteps.size > 0) {
      cmd := ["fan", ctx.script].addAll(ctx.myargs)
      cmd.addAll(["-debug_more", "-job", "$id-m"])
      echo(cmd.join(" "))
      i := Process(cmd).run.join
      if (i != 0) {
        throw Err("run script error: $i")
      }
    }

    if (reduceSteps.size > 0) {
      cmd2 := ["fan", ctx.script].addAll(ctx.myargs)
      cmd2.addAll(["-debug_more", "-job", "$id-r"])
      echo(cmd2.join(" "))
      i2 := Process(cmd2).run.join
      if (i2 != 0) {
        throw Err("run script error: $i2")
      }
    }

    isDone = true
    return true
  }
}

internal class LocalWorkJob : Job {
  Bool client := true

  new make(Context c, Int id, Job? parent) : super.make(c, id, parent) {
  }

  override Bool run() {
    if (isDone) return true

    if (client) parent?.run

    runSteps(mapSteps, true)
    runSteps(reduceSteps, false)

    isDone = true
    return true
  }

  protected virtual Bool runSteps(DataSet[] steps, Bool isMapper) {
    if (steps.size == 0) {
      return false
    }

    Uri[] ins := steps.first.input
    out := steps.last.output[0].toFile.out

    lst := Str[,]
    ins.each |file| {
      for (i:=0; i<steps.size; ++i) {
        step := steps[i].processor
        if (step is JoinMapper) {
          JoinMapper mapper := step
          mapper.init(file, steps[i].joinInput)
        }
      }
      file.toFile.in.eachLine |line| {
        if (!line.isEmpty) {
          Str? res := line
          for (i:=0; i<steps.size; ++i) {
            step := steps[i].processor
            res = step.call(res)
            if (res == null || res.isEmpty) break
          }
          if (res != null && !res.isEmpty) {
            lst.addAll(res.splitLines)
          }
        }
      }
    }
    //sort
    if (isMapper) {
      lst.sort |v1, v2->Int| {
        k1 := v1.split('\t').getSafe(0, "")
        k2 := v2.split('\t').getSafe(0, "")
        if (sortType != null) return sort(k1, k2, sortType)
        return k1 <=> k2
      }
    }

    if (steps.size > 0) {
      Str? res := steps.last.processor.end
      if (res != null && !res.isEmpty) {
        lst.addAll(res.splitLines)
      }
    }

    //wirite
    lst.each {
      out.printLine(it)
    }
    out.close
    return true
  }

  private Int sort(Str v1, Str v2, Str sortType) {
      r := sortType.containsChar('r')
      res := 0
      if (sortType.containsChar('n')) {
        if (v1.containsChar('.') || v2.containsChar('.')) {
            k1 := v1.toFloat
            k2 := v2.toFloat
            res = k1 <=> k2
        }
        else {
            k1 := v1.toInt
            k2 := v2.toInt
            res = k1 <=> k2
        }
      }
      else {
        res = v1 <=> v2
      }
      if (r) res = -res
      return res
  }
}
