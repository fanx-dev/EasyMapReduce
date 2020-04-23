
using easyMapReduce

class Main
{
  Context c := Context(this)
  Uri uri := `/home/test/`
  Uri input := `./testData/test.txt`
  Uri output := `./testData/wordCount`
  Uri tempDir := `./temp/`

  private Void init() {
    if (!c.isDebug) {
      input = uri + input
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

    ds.reduceTaskNum = 1
    ds.save(output)
  }

}
