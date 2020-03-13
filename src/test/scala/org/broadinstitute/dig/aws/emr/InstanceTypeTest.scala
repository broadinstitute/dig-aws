package org.broadinstitute.dig.aws.emr

import org.scalatest.FunSuite
import org.json4s._

/**
 * @author clint
 * Mar 13, 2020
 */
final class InstanceTypeTest extends FunSuite {
  test("deserialize") {
    import InstanceType._
    import org.json4s.jackson.Serialization.read
    
    def doTest(expected: InstanceType, serialized: String): Unit = {
      implicit val formats = DefaultFormats
      
      assert(deserialize(read[JValue](serialized)) === expected)
      
      ()
    }
    
    doTest(m5_2xlarge, quote("m5.2xlarge"))
    doTest(m5_4xlarge, quote("m5.4xlarge"))
    doTest(m5_12xlarge, quote("m5.12xlarge"))
    doTest(m5_24xlarge, quote("m5.24xlarge"))
  
    doTest(r5_2xlarge, quote("r5.2xlarge"))
    doTest(r5_4xlarge, quote("r5.4xlarge"))
    doTest(r5_8xlarge, quote("r5.8xlarge"))
    doTest(r5_12xlarge, quote("r5.12xlarge"))
  
    doTest(c5_2xlarge, quote("c5.2xlarge"))
    doTest(c5_4xlarge, quote("c5.4xlarge"))
    doTest(c5_9xlarge, quote("c5.9xlarge"))
    doTest(c5_18xlarge, quote("c5.18xlarge"))
  }
  
  private def quote(s: String): String = s""""${s}""""
}
