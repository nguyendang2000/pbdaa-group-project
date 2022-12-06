{
    println("NYC 2020 MEDIAN INCOME DATA PROFILING")

    val csvFile = sc.textFile("/user/nd1663/project/datasets/median_income_clean.csv")

    println("\nNumber of records (with header): " + csvFile.count())

    val headerRow = csvFile.first()

    println("Number of columns: " + headerRow.split(",").size + "")

    val noHeader = csvFile.filter(row => !row.equals(headerRow))

    println("Number of records (excluding header): " + noHeader.count() + "\n")

    var i = 0

    println("Number of records where income data is not available, by category:")
    for(i <- 2 to 80 by 2) {
        val emptyRows = noHeader.map(row => {
            if(row.split(",")(i).toInt == -1) {
                1
            }
            else {
                0
            }
        })
        val count = emptyRows.sum()
        println(f"${headerRow.split(",")(i).trim()}: ${count.toInt}")
    }
}

