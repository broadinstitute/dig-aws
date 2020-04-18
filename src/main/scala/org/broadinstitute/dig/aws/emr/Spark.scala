package org.broadinstitute.dig.aws.emr

/** Spark ApplicationConfig classifications and properties.
  */
object Spark {

  /** Core spark configuration. */
  final case class Config(
      override val classificationProperties: Seq[ClassificationProperties] = Seq.empty,
      override val properties: Seq[(String, String)] = Seq.empty,
  ) extends ApplicationConfig[Config] {
    override val classification: String = "spark"

    /** Have AWS pick good memory numbers for executors, drivers, etc. */
    def withMaximizeResourceAllocation: Config =
      withProperty("maximizeResourceAllocation" -> "true")
  }

  /** ApplicationConfig for spark-defaults. */
  final case class Defaults(
     override val classificationProperties: Seq[ClassificationProperties] = Seq.empty,
     override val properties: Seq[(String, String)] = Seq.empty,
   ) extends ApplicationConfig[Defaults] {
    override val classification: String = "spark-defaults"

    /** Set the memory for each executor. */
    def withExecutorMemory(mem: MemorySize): Defaults =
      withProperty("spark.executor.memory" -> mem.toString)

    /** Set the cores for each executor. */
    def withExecutorCores(n: Int): Defaults =
      withProperty("spark.executor.cores" -> n.toString)

    /** Set the memory overhead for each executor. */
    def withExecutorMemoryOverhead(mem: MemorySize): Defaults =
      withProperty("spark.executor.memoryOverhead" -> mem.toString)

    /** Set the memory overhead for the yarn executor. */
    def withExecutorYarnMemoryOverhead(mem: MemorySize): Defaults =
      withProperty("spark.yarn.executor.memoryOverhead" -> mem.toString)

    /** Set the memory for each driver. */
    def withDriverMemory(mem: MemorySize): Defaults =
      withProperty("spark.driver.memory" -> mem.toString)

    /** Set the cores for each driver. */
    def withDriverCores(n: Int): Defaults =
      withProperty("spark.driver.cores" -> n.toString)

    /** Set the memory overhead for each driver. */
    def withDriverMemoryOverhead(mem: MemorySize): Defaults =
      withProperty("spark.driver.memoryOverhead" -> mem.toString)
  }

  /** ApplicationConfig for spark-env. */
  final case class Env(
    override val classificationProperties: Seq[ClassificationProperties] = Seq.empty,
    override val properties: Seq[(String, String)] = Seq.empty,
  ) extends ApplicationConfig[Env] {
    override val classification: String = "spark-env"

    /** Classification properties for exporting environment variables to use python 3. */
    def withPython3: Env = {
      val export = ClassificationProperties("export")
        .withProperty("PYSPARK_PYTHON" -> "/usr/bin/python3")

      withClassificationProperties(export)
    }
  }
}
