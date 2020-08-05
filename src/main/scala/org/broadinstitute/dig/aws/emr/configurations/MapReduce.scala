package org.broadinstitute.dig.aws.emr.configurations

import org.broadinstitute.dig.aws.{MB, MemorySize}

/** MapReduce classifications, properties, and configurations. */
object MapReduce {

  /** Classification object for mapred-site. */
  class Site extends Configuration("mapred-site") {

    /** Set the amount of memory allocated for map jobs. */
    def mapMemory(mem: MemorySize): this.type = {
      val jvmMB = MemorySize(mem.toMB.size * 80 / 100, MB)

      this
        .addProperty("mapreduce.map.java.opts" -> s"-Xmx${jvmMB.toString}")
        .addProperty("mapreduce.map.memory.mb" -> mem.toMB.size.toString)
    }

    /** Set the amount of memory allocated for reduce jobs. */
    def reduceMemory(mem: MemorySize): this.type = {
      val jvmMB = MemorySize(mem.toMB.size * 80 / 100, MB)

      this
        .addProperty("mapreduce.reduce.java.opts" -> s"-Xmx${jvmMB.toString}")
        .addProperty("mapreduce.reduce.memory.mb" -> mem.toMB.size.toString)
    }
  }
}
