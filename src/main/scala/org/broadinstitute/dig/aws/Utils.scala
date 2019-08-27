package org.broadinstitute.dig.aws

import cats.effect._
import cats.syntax.all._
import fs2._


/** Utility functions. */
object Utils {
  /** Helper to allow performing an IO operation, but ignore the results. */
  val ignoreIO: IO[_] => IO[Unit] = _.map(scala.Function.const(()))

  /** Given a sequence of IO tasks, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    *
    * Optionally, apply a mapping function for each.
    */
  def waitForTasks[A, R](tasks: Seq[IO[A]], limit: Int = 5)
                        (mapEach: IO[A] => IO[R] = ignoreIO)
                        (implicit contextShift: ContextShift[IO] = Implicits.Defaults.contextShift): IO[Unit] = {
    Stream
      .emits(tasks)
      .covary[IO]
      .mapAsyncUnordered(limit)(mapEach)
      .compile
      .toList
      .as(())
  }
}
