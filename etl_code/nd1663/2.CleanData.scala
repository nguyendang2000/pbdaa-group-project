{
println("NYC 2020 MEDIAN INCOME DATA CLEANING")

val NUMBER_COLS:Array[Int] = Array[Int](2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94, 98, 102, 106, 110, 114, 118, 122, 126, 130, 134, 138, 142, 146, 150, 154, 158)
val INCOME_COLS:Array[Int] = Array[Int](322, 326, 330, 334, 338, 342, 346, 350, 354, 358, 362, 366, 370, 374, 378, 382, 386, 390, 394, 398, 402, 406, 410, 414, 418, 422, 426, 430, 434, 438, 442, 446, 450, 454, 458, 462, 466, 470, 474, 478)

val NUMBER_COLS_SHORT:Array[Int] = Array[Int](2)
val INCOME_COLS_SHORT:Array[Int] = Array[Int](322)

def parseData(colData: String) : Int = {
    if(colData.equals("250,000+")) {
        250000
    }
    else if(colData.contains("*") || colData.equals("\"-\"") || colData.equals("\"null\"")) {
        -1
    }
    else {
        colData.replaceAll("\"", "").toInt
    }
}

def zipInNYC(zipCode: String) : Boolean = {
    /* REFERENCE ZIP CODES IN NYC BOROUGHS
    Manhattan: 10001-10282
    Staten Island: 10301-10314
    Bronx: 10451-10475
    Queens: 11004-11109, 11351-11697
    Brooklyn: 11201-11256        
    */
    val zip:Int = zipCode.toInt
    if( (zip >= 10001 && zip <= 10282) || (zip >= 10301 && zip <= 10314) ||
        (zip >= 10451 && zip <= 10475) || (zip >= 11004 && zip <= 11109) ||
        (zip >= 11351 && zip <= 11697) || (zip >= 11201 && zip <= 11256)) {
            true
    }
    else {
        false
    }
}

val csvFile = sc.textFile("/user/nd1663/project/datasets/median_income_raw_data.csv")
val headerOne = csvFile.first()
val noHeaderOne = csvFile.filter(row => !row.equals(headerOne))
val headerTwo = noHeaderOne.first();
val noHeaderTwo = noHeaderOne.filter(row => !row.equals(headerTwo))

val filteredData = noHeaderTwo.filter(row => {
    val zipCode = row.split(",")(1).substring(7, 12)
    zipInNYC(zipCode)
})

val cleanData = filteredData.map(row => {
    val array = row.split(",")
    var rowData:String = row.split(",")(1).substring(7, 12) + ","
    var i = 0
    for(i <- 0 to NUMBER_COLS.length - 1) {
        val noHouseholds = parseData(array(NUMBER_COLS(i)))
        var medianIncome = parseData(array(INCOME_COLS(i)))
        if(medianIncome != -1 && medianIncome <= 1000) {
            medianIncome = medianIncome * 1000; // Fixes issue with Java sometimes not parsing properly, e.g. '250,000+' becomes 250
        }
        if(i == NUMBER_COLS.length - 1) {
            rowData = rowData.concat(noHouseholds + "," +  medianIncome)
        }
        else {
            rowData = rowData.concat(noHouseholds + "," +  medianIncome + ",")            
        }
    }
    rowData
})

val cleanDataShort = filteredData.map(row => {
    val array = row.split(",")
    var rowData:String = row.split(",")(1).substring(7, 12) + ","
    var i = 0
    for(i <- 0 to NUMBER_COLS_SHORT.length - 1) {
        val noHouseholds = parseData(array(NUMBER_COLS_SHORT(i)))
        var medianIncome = parseData(array(INCOME_COLS_SHORT(i)))
        if(medianIncome != -1 && medianIncome <= 1000) {
            medianIncome = medianIncome * 1000; // Fixes issue with Java sometimes not parsing properly, e.g. '250,000+' becomes 250
        }
        if(i == NUMBER_COLS_SHORT.length - 1) {
            rowData = rowData.concat(noHouseholds + "," +  medianIncome)
        }
        else {
            rowData = rowData.concat(noHouseholds + "," +  medianIncome + ",")            
        }
    }
    rowData
})

cleanData.coalesce(1, true).saveAsTextFile("/user/nd1663/project/datasets/median_income_clean")

cleanDataShort.coalesce(1, true).saveAsTextFile("/user/nd1663/project/datasets/median_income_clean_short")

println("DATA CLEANING FINISHED")
}