package org.broadinstitute.dig.aws.compat

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.language.higherKinds

/**
 * @author clint
 * May 13, 2021
 * 
 * Provide some methods from the 2.13 stdlib to 2.12 code that aren't provided by scala-collection-compat.
 */
object Shims {
  /**
   * Provide Scala 2.13's Growable.addAll().
   */
  implicit final class GrowableOps[A, G[X] <: Growable[X]](val buffer: G[A]) extends AnyVal {
    /**
     * Add all elements produced by a TraversableOnce to the wrapped buffer.
     *
     *  @param as   the TraversableOnce producing the elements to add.
     *  @return  the wrapped collection itself.
     */
    def addAll(as: TraversableOnce[A]): G[A] = buffer ++= as
  }

  /**
   * Provide Scala 2.13's mutable.Map.updateWith().
   */
  implicit final class MutableMapOps[K, V](val m: mutable.Map[K, V]) extends AnyVal {
    /**
     * NB: taken from https://github.com/scala/scala/blob/v2.13.5/src/library/scala/collection/mutable/Map.scala
     *
     * Update a mapping for the specified key and its current optionally-mapped value
     * (`Some` if there is current mapping, `None` if not).
     *
     * If the remapping function returns `Some(v)`, the mapping is updated with the new value `v`.
     * If the remapping function returns `None`, the mapping is removed (or remains absent if initially absent).
     * If the function itself throws an exception, the exception is rethrown, and the current mapping is left
     * unchanged.
     *
     * @param key the key value
     * @param remappingFunction a partial function that receives current optionally-mapped value and return a new
     * mapping
     * @return the new value associated with the specified key
     */
    def updateWith(key: K)(remappingFunction: Option[V] => Option[V]): Option[V] = {
      val previousValue = m.get(key)
      val nextValue = remappingFunction(previousValue)
      (previousValue, nextValue) match {
        case (None, None) => // do nothing
        case (Some(_), None) => m.remove(key)
        case (_, Some(v)) => m.update(key, v)
      }
      nextValue
    }
  }
}