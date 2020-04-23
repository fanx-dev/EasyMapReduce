//
// Copyright (c) 2017, chunquedong
// Licensed under the Apache Licene 2.0
// History:
//   2017-7-1  Jed Young  Creation
//

using build
class Build : build::BuildPod
{
  new make()
  {
    podName = "easyMapReduce"
    summary = "hadoop map reduce with spark style"
    srcDirs = [`fan/`, `test/`]
    depends = ["sys 2.0", "std 1.0"]
  }
}