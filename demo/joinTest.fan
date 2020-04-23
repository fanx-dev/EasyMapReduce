
using easyMapReduce

class Main
{
  Context c := Context(this)
  Uri uri := `/home/test/`
  Uri input := `./testData/test.txt`
  Uri input2 := `./testData/test2.txt`
  Uri output := `./testData/joinTest`
  Uri tempDir := `./temp/`

  private Void init() {
    if (!c.isDebug) {
      input = uri + input
      input2 = uri + input2
      output = uri + output
      tempDir = uri + tempDir
    }

    c.tempDir = tempDir
    c.deleteDir(tempDir)
    c.deleteDir(output)
  }

  Void main() {
    init

    ds := c.load(input).map{ it.split(' ').join("\n") }.reduce(0)|Int r,v,k|{ r + 1 }
    ds2 := c.load(input2)

    joined := ds.join(ds2)
    joined.save(output)
  }

}