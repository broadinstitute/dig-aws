package org.broadinstitute.dig.aws.emr

/** Memory units would be bytes, megabytes, gigabytes, etc. */
abstract case class MemoryUnit(unit: String, unitsPerBytes: Int)

/** Common memory units. */
object B extends MemoryUnit("b", 1)
object MB extends MemoryUnit("m", 1024 * 1024)
object GB extends MemoryUnit("g", 1024 * 1024 * 1024)

/** Handles ApplicationConfig memory size strings for EMR clusters. */
final case class MemorySize(size: Int, units: MemoryUnit) {
  override val toString: String = s"$size${units.unit}"

  /** Convert from current units to bytes. */
  def toB: MemorySize = MemorySize(size * units.unitsPerBytes, B)

  /** Convert to megabytes. */
  def toMB: MemorySize = MemorySize(size * (units.unitsPerBytes / MB.unitsPerBytes), MB)

  /** Convert to gigabytes. */
  def toGB: MemorySize = MemorySize(size * (units.unitsPerBytes / GB.unitsPerBytes), GB)
}

/** Companion object with implicits. */
object MemorySize {
  object Implicits {
    final implicit class Sizes[A](val size: Int) extends AnyVal {

      /** Convert an integer to bytes. */
      def b: MemorySize = MemorySize(size, B)

      /** Convert an integer to megabytes. */
      def mb: MemorySize = MemorySize(size, MB)

      /** Convert an integer to gigabytes. */
      def gb: MemorySize = MemorySize(size, GB)
    }
  }
}
