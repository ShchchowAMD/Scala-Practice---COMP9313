package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem2 {

	def main(args: Array[String]) {
		val inputFile = args(0)
		val outputFolder = args(1)
		val k = args(2).toInt
		val conf = new SparkConf().setAppName("Problem2").setMaster("local")
		val sc = new SparkContext(conf)
		val textFile = sc.textFile(inputFile).map(_.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+")) //split the txt contents as the required format
		val newTextFile = textFile.map(_.filter(x => (x.length >0 && x.charAt(0) <= 'z' && x.charAt(0) >= 'a'))) //filter the data that has elements and the element's first character is in [a~z]
		val coTerm = newTextFile.flatMap{ line =>
		for{ 
			i <-0 until line.length
			j <- (i+1) until line.length
		} yield {
			if(line(i) < line(j)) {
				((line(i), line(j)), 1)
			}
			else {
				((line(j), line(i)), 1)
			}
		}} //use twice for loop to combine two elements and use yield to save the two elements in special format, the small element put at the head and set 1 to express the number of the combine string
		val reduces = coTerm.reduceByKey(_+_) //use reduceByKey function to add the same key's value
		val results = reduces.map(_.swap).sortByKey(false).take(k).map(x => (x._2._1 + "," + x._2._2 + "\t" + x._1)) //swap the key and value then sort in descending order and then map the key and value in the required format
		val finalResults = sc.makeRDD(results) //use makeRDD() function to make the Array[String] be the RDD
		finalResults.saveAsTextFile(outputFolder) //use saveAsTextFile to write the answer in the outputFolder
	}
}