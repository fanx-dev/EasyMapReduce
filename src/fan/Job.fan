//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

abstract class Job {
  protected Int id { private set }
  protected Context ctx { private set }
  protected DataSet[] mapSteps := [,] { private set }
  protected DataSet[] reduceSteps := [,] { private set }
  protected Job? parent { private set }
  protected Bool isDone := false

  protected Str? sortType
  protected Str? partitioner
  protected Bool isCount := false

  new make(Context c, Int id, Job? parent) {
    ctx = c
    this.id = id
    this.parent = parent
  }

  override Str toStr() {
    "$id,$isDone,${mapSteps}->${reduceSteps}"
  }

  abstract Bool run()
}

internal class ClientJob : Job {
  Bool enable := true

  new make(Context c, Int id, Job? parent) : super.make(c, id, parent) {
  }

  override Bool run() {
    if (isDone) return true
    parent?.run

    HADOOP_HOME := Env.cur.vars.get("HADOOP_HOME")
    if (HADOOP_HOME == null) {
      if (enable)
        throw Err("required env var HADOOP_HOME")
      else
        HADOOP_HOME = "/usr/bin/hadoop/software/hadoop"
    }

    cs := "$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming.jar"
    cmd := cs.split(' ')

    if (mapSteps.first?.compression != null) {
      gzip := mapSteps.first?.compression
      cmd.add("-D").add("stream.recordreader.compression=$gzip")
    }

    if (reduceSteps.isEmpty) {
      cmd.add("-D").add("mapred.reduce.tasks=0")
    }
    else if (reduceSteps.last.reduceTaskNum != null) {
      cmd.add("-D").add("mapred.reduce.tasks=$reduceSteps.last.reduceTaskNum")
    }

    if (ctx.name != "") {
      cmd.add("-D").add("mapred.job.name='$ctx.name'")
    }

    if (sortType != null) {
        //cmd.add("-D").add("stream.num.map.output.key.fields=2")
        //cmd.add("-D").add("map.output.key.field.separator=:")
        //cmd.add("-D").add("mapreduce.map.output.key.field.separator=:")
        cmd.add("-D").add("mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator")
        cmd.add("-D").add("mapreduce.partition.keycomparator.options=$sortType")
    }
    if (partitioner != null) {
        cmd.add("-D").add("mapreduce.partition.keypartitioner.options=$partitioner")
    }

    files := ""
    if (ctx.fanHome != null) {
      files = ctx.fanHome.toStr
    }
    args := ctx.hadoopArgs.dup
    for (i:=0; i < args.size; ++i) {
        if (args[i] == "-files") {
            args.removeAt(i)
            if (i < args.size) {
                files += "," + args[i]
                args.removeAt(i)
            }
            --i
        }
    }
    if (files.size > 0) {
        cmd.add("-files").add(files)
    }

    //------------------------------------------
    //Be sure to place the generic options before the streaming options
    //-------------------------------------------
    args.each {
      cmd.add(it)
    }

    if (partitioner != null) {
        cmd.add("-partitioner").add("org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    }

    if (ctx.script != null) {
      cmd.add("-file").add(ctx.script)
    }

    if (mapSteps.isEmpty) {
      reduceSteps.first?.input?.each { cmd.add("-input").add(it.toStr) }
    } else {
      mapSteps.first?.input?.each { cmd.add("-input").add(it.toStr) }
    }

    if (reduceSteps.isEmpty) {
      mapSteps.last?.output?.each { cmd.add("-output").add(it.toStr) }
    } else {
      reduceSteps.last?.output?.each { cmd.add("-output").add(it.toStr) }
    }

    myargs := ctx.myargs.join(" ")
    if (mapSteps.size > 0) {
      cmd.add("-mapper").add("sh ./${ctx.fanHome.name}/bin/fan $ctx.script $myargs -job $id-m")
    } else {
      cmd.add("-mapper").add("org.apache.hadoop.mapred.lib.IdentityMapper")
    }

    if (reduceSteps.size > 0) {
      cmd.add("-reducer").add("sh ./${ctx.fanHome.name}/bin/fan $ctx.script $myargs -job $id-r")
    }

    cmdStr := cmd.join(" ") |v| {
        if (v.contains(" ")) return "'$v'"
        return v
    }
    echo(cmdStr)

    if (enable) {
      i2 := Process(cmd).run.join
      if (i2 != 0) {
        throw Err("run script error: $i2")
      }
    }

    isDone = true
    return true
  }
}

class WorkJob : Job {
  new make(Context c, Int id, Job? parent) : super.make(c, id, parent) {
  }

  override Bool run() {
    if (isDone) return true
    Bool isMapper := ctx.isMapper
    ctx.print("run $id, $isMapper")
    res := isMapper ? runSteps(mapSteps, isMapper) : runSteps(reduceSteps, isMapper)
    isDone = true
    return res
  }

  protected virtual Bool runSteps(DataSet[] steps, Bool isMapper) {
    if (steps.size == 0) {
      return false
    }

    in := Env.cur.in
    out := Env.cur.out

    Bool needCurFile := false
    Str? currentFile := null
    for (i:=0; i<steps.size; ++i) {
      step := steps[i].processor
      if (step is JoinMapper) {
        needCurFile = true
      }
    }

    if (isCount) {
        count := 0
        while(in.readLine != null) {
            ++count
        }
        out.printLine(count)
        return true
    }

    while(true) {
      Str? line
      try {
        line = in.readLine
        if (line == null) break
      } catch  {
        continue
      }
      if (!line.isEmpty) {
        if (needCurFile) {
          file := Env.cur.vars["mapreduce_map_input_file"]
          if (file == null) {
            file = Env.cur.vars["map_input_file"]
          }
          fileChanged := false
          if (file != null) {
            if (currentFile == null || file != currentFile) {
              currentFile = file
              fileChanged = true
            }
          }
          if (fileChanged) {
            for (i:=0; i<steps.size; ++i) {
              step := steps[i].processor
              if (step is JoinMapper) {
                JoinMapper mapper := step
                mapper.init(currentFile.toUri, steps[i].joinInput)
              }
            }
          }
        }

        Str? res := line
        for (i:=0; i<steps.size; ++i) {
          step := steps[i].processor
          res = step.call(res)
          if (res == null || res.isEmpty) break
        }
        if (res != null && !res.isEmpty) {
          out.printLine(res)
        }
      }
    }

    if (steps.size > 0) {
      Str? res := steps.last.processor.end
      if (res != null && !res.isEmpty) {
        out.printLine(res)
      }
    }

    out.flush
    return true
  }

}

