package execute

import FileSources.readCSV

object executeMain {
  def main(args: Array[String]): Unit = {
    var csvPath = "datasets/C2ImportSchoolSample.csv"
    var csvPathOut = "datasets/C2ImportSchoolSample.csv"
    val csv = new readCSV()
    println("Reading CSV files")
    val csvDF = csv.read(csvPath)
    csvDF.show(false)
    csv.write(csvDF, csvPathOut)
    println("Writing CSV files")
  }
}
