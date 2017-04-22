package shipper

import java.io._
import org.tsers.zeison.Zeison
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext

class BigShipperTests extends FunSuite with SharedSparkContext{
	import org.apache.spark.sql.types._
	System.setProperty("loglevel", "FATAL")
	test("Checked utils.makeSchema") {
		val loader = new Loader(sc)
		val expected = new StructType(Array(StructField("field1",IntegerType,true), StructField("field2",StringType,true), StructField("field3",BooleanType,true), StructField("field4",TimestampType,true), StructField("field5",FloatType,true)))
		val result = loader.utils.makeSchema("field1:int,field2:string,field3:boolean,field4:date,field5:float")
		assert(result == expected)
	}
	test("Checked utils.fieldTypeInSchema") {
		val loader = new Loader(sc)
		val expected = LongType
		val result = loader.utils.fieldTypeInSchema("bigint")
		assert(result == expected)
	}
}
