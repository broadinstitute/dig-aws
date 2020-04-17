package org.broadinstitute.dig.aws.emr

/** Memory units would be bytes, megabytes, gigabytes, etc. */
abstract case class MemoryUnit(unit: String)

/** Common memory units. */
object Bytes extends MemoryUnit("b")
object Megabytes extends MemoryUnit("m")
object Gigabytes extends MemoryUnit("g")

/** Handles ApplicationConfig memory size strings for EMR clusters. */
final case class MemorySize(size: Int, units: MemoryUnit) {
  override val toString: String = s"${size}${units.unit}"
}

/** Companion object with implicits. */
object MemorySize {
  object Implicits {
    final implicit class Sizes[A](val size: Int) extends AnyVal {

      /** Convert an integer to bytes. */
      def b: MemorySize = MemorySize(size, Bytes)

      /** Convert an integer to megabytes. */
      def mb: MemorySize = MemorySize(size, Megabytes)

      /** Convert an integer to gigabytes. */
      def gb: MemorySize = MemorySize(size, Gigabytes)
    }
  }
}
