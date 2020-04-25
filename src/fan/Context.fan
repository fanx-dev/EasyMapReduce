//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

class Context {
  private Int runJobId := -1
  internal Bool isMapper := true { private set }

  //0:run in hadoop, 1:run in new process 2: direct run 3: show job args
  private Int runMode := 0 { private set }

  private Job[] jobs := [,]
  internal Int stepCount := 0

  ** temp dir to save middle temp file
  Uri tempDir := `./temp/`
  internal Str? script { private set }

  ** job name
  Str name { private set }

  Str hadoopHome := "HADOOP_HOME"

  ** A light-weight(delete src and swt.jar) Fantom distro to be upload to Hadoop
  Uri? fanHome

  ** args for hadoop job. setting by -hadoop command arg
  Str[] hadoopArgs := [,]

  ** app args. exclude hadoop job args
  Str[] myargs := [,]

  new make(Obj? script, Str name := "") {
    this.name = name
    if (script != null) {
      try {
        this.script = Uri(script.typeof->sourceFile).name
        if (name == "") {
            this.name = Uri(script.typeof->sourceFile).basename
        }
      } catch (Err e) { e.traceTo(Env.cur.err) }
    }

    args := Env.cur.args
    for (i:=0; i<args.size; ++i) {
      arg := args[i]

      if (arg == "-debug") {
        runMode = 2
        continue
      }
      else if (arg == "-debug_more") {
        runMode = 1
        continue
      }
      else if (arg == "-debug_show") {
        runMode = 3
        continue
      }
      else if (arg == "-hadoop") {
        ++i
        if (i < args.size) {
          hadoopArgs = args[i].split(' ')
        }
      }
      else if (arg == "-job") {
        ++i
        if (i < args.size) {
          fs := args[i].split('-')
          runJobId = fs[0].toInt
          isMapper = fs[1] == "m"
        }
      }
      else {
        myargs.add(arg)
      }
    }

    f := tempDir.toFile.create

    fanHomeStr := Context#.pod.config("fanHome")
    if (fanHomeStr == null) {
      fanHome = Env.cur.homeDir.uri
    }
    else {
      fanHome = fanHomeStr.toUri
      //check fanHome
      fanDir := File(fanHome)
      if (!isDebug && !fanDir.exists) {
        throw ArgErr("Not config fanHome:$fanHome to upload")
      }
    }

    HADOOP_HOME := Env.cur.vars.get("HADOOP_HOME")
    if (HADOOP_HOME != null) {
      hadoopHome = HADOOP_HOME
    }
  }

  ** delete dir if in client mode
  Void deleteDir(Uri uri) {
    if (!isClient) return
    if (runMode == 0) {
      try Process("$hadoopHome/bin/hadoop fs -rmr $uri".split(' ')).run.join
      catch (Err e) e.trace
    } else {
      uri.toFile.delete
    }
  }

  ** client mode is job handle in local
  Bool isClient() { runJobId == -1 }

  ** debug mode to simulate Hadoop job in local
  Bool isDebug() { runMode == 1 || runMode == 2 }

  Void print(Obj? o) {
    if (isClient) echo(o)
    else Env.cur.err.printLine("e: $o")
  }

  DataSet load(Uri input) {
    DataSet(this, [input])
  }

  private Job newJob(Job? parent) {
    id := jobs.size
    Job? job
    if (runMode == 0 || runMode == 3) {
      if (isClient) {
        job = ClientJob(this, id, parent)
        (job as ClientJob).enable = runMode == 0
      } else {
        job = WorkJob(this, id, parent)
      }
    } else {
      if (isClient) {
        if (runMode == 1) {
          job = LocalClientJob(this, id, parent)
        } else {
          job = LocalWorkJob(this, id, parent)
          (job as LocalWorkJob).client = true
        }
      } else {
        job = LocalWorkJob(this, id, parent)
        (job as LocalWorkJob).client = false
      }
    }
    jobs.add(job)
    return job
  }

  private Void dependSteps(DataSet ds, DataSet[] depends) {
    if (ds.job != null) return

    if (ds.parent != null) {
      dependSteps(ds.parent, depends)
    }

    if (ds.joinWith != null) {
      dependSteps(ds.joinWith, depends)
    }

    depends.add(ds)
  }

  private Job genJobs(DataSet[] steps) {
    jobs := Job[,]

    Job? parentJob
    if (steps[0].parent != null) {
      parentJob = steps[0].parent.job
    }
    job := newJob(parentJob)
    jobs.add(job)

    for (i:=0; i<steps.size; ++i) {
      step := steps[i]
      if (step.processor == null || step.job != null) continue

      if (step.isMapper) {
        if (job.reduceSteps.isEmpty && step.joinWith == null) {
          job.mapSteps.add(step)
        }
        else {
          job = newJob(job)
          jobs.add(job)
          job.mapSteps.add(step)
        }
        if (step.sortType != null) job.sortType = step.sortType
      } else {
        if (job.reduceSteps.isEmpty) {
          job.reduceSteps.add(step)
        }
        else {
          job = newJob(job)
          jobs.add(job)
          job.reduceSteps.add(step)
        }
      }
      step.job = job
    }
    //echo(jobs)
    return job
  }

  internal Void submit(DataSet ds) {
    if (ds.job != null) return

    steps := DataSet[,]
    dependSteps(ds, steps)
    job := genJobs(steps)

    if (!isClient) {
      if (runJobId < jobs.size) {
        this.jobs[runJobId].run
      }
    } else {
      job.run
    }
  }
}

