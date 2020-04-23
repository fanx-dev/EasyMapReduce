
using easyMapReduce

class Main
{
  Context c := Context(this)
  Uri uri := `/home/test/`
  Uri input := `./testData/test3.txt`
  Uri output := `./testData/count_out`
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
    ds := c.load(input)
    lineCount := ds.count
    c.print(lineCount)
  }

}
