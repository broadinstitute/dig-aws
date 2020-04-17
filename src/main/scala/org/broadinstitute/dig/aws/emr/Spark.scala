package org.broadinstitute.dig.aws.emr

/** ApplicationConfig for the spark. */
object Spark {
  val config: ApplicationConfig = ApplicationConfig("spark")

  /** Maximize memory resources for executors. */
  val maximizeResourceAllocation: ApplicationConfig.Property = "maximizeResourceAllocation" -> "true"

  /** ApplicationConfig settings for spark-defaults. */
  object Defaults {
    val config: ApplicationConfig = ApplicationConfig("spark-defaults")

    /** ApplicationConfig spark.executor.* properties. */
    object Executor {
      /** Set the memory for each executor. */
      def memory(mem: MemorySize): ApplicationConfig.Property = "spark.executor.memory" -> mem.toString

      /** Set the cores for each executor. */
      def cores(n: Int): ApplicationConfig.Property = "spark.executor.cores" -> n.toString
    }

    /** ApplicationConfig spark.driver.* properties. */
    object Driver {
      /** Set the memory for each driver. */
      def memory(mem: MemorySize): ApplicationConfig.Property = "spark.driver.memory" -> mem.toString

      /** Set the cores for each driver. */
      def cores(n: Int): ApplicationConfig.Property = "spark.driver.cores" -> n.toString
    }
  }

  /** ApplicationConfig settings for spark-env. */
  object Env {
    val config: ApplicationConfig = ApplicationConfig("spark-env")

    /** Exported spark environment variables. */
    object Export {
      val properties: ClassificationProperties = ClassificationProperties("export")

      /** Uses Python3 for the environment. */
      val usePython3: ApplicationConfig.Property ="PYSPARK_PYTHON" -> "/usr/bin/python3"
    }
  }
}
