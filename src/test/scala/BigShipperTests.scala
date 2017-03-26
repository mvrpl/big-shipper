package shipper

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext

class BigShipperTests extends FunSuite with SharedSparkContext{
	test("First test") {
		assert(1 == 1)
	}
}
