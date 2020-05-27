package org.broadinstitute.dig.aws.emr.configurations

/** Spark classifications, properties, and configurations.
  */
object Spark {

  /** Classification for spark. */
  class Config extends Configuration("spark") {

    /** Have AWS pick good memory numbers for executors, drivers, etc. */
    def maximizeResourceAllocation(): this.type = {
      addProperty("maximizeResourceAllocation" -> "true")
    }
  }

  /** Classification for spark-defaults. */
  class Defaults extends Configuration("spark-defaults") {

    /** Set the memory for each executor. */
    def executorMemory(mem: MemorySize): this.type = {
      addProperty("spark.executor.memory" -> mem.toString)
    }

    /** Set the cores for each executor. */
    def executorCores(n: Int): this.type = {
      addProperty("spark.executor.cores" -> n.toString)
    }

    /** Set the memory overhead for each executor. */
    def executorMemoryOverhead(mem: MemorySize): this.type = {
      addProperty("spark.executor.memoryOverhead" -> mem.toString)
    }

    /** Set the memory overhead for the yarn executor. */
    def executorYarnMemoryOverhead(mem: MemorySize): this.type = {
      addProperty("spark.yarn.executor.memoryOverhead" -> mem.toString)
    }

    /** Set the memory for each driver. */
    def driverMemory(mem: MemorySize): this.type = {
      addProperty("spark.driver.memory" -> mem.toString)
    }

    /** Set the cores for each driver. */
    def driverCores(n: Int): this.type = {
      addProperty("spark.driver.cores" -> n.toString)
    }

    /** Set the memory overhead for each driver. */
    def driverMemoryOverhead(mem: MemorySize): this.type = {
      addProperty("spark.driver.memoryOverhead" -> mem.toString)
    }
  }

  /** Classification for spark-env. */
  class Env extends Configuration("spark-env") with Export[Env] {

    /** Classification properties for exporting environment variables to use python 3. */
    def usePython3(): this.type = {
      export("PYSPARK_PYTHON" -> "/usr/bin/python3")
    }
  }
}
