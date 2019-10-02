package org.broadinstitute.dig.aws

import scala.language.higherKinds

import cats.Id
import cats.effect.IO
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import cats.effect.Timer
import cats.Monad
import cats.Traverse
import cats.Parallel
import cats.Applicative
import cats.effect.ContextShift
import cats.effect.Clock
import fs2.Stream
import cats.effect.Concurrent

/**
 * @author clint
 * Oct 1, 2019
 */
sealed trait AwsOps[F[_]] {
  def pure[A](a: => A): F[A]
  
  def sleep(d: FiniteDuration)(implicit timer: Timer[F]): F[Unit]
  
  def raiseError(t: Throwable): F[Nothing] 
  
  protected def functorMonadOps: AwsOps.FunctorMonadOps[F]
  
  protected def bracketOps: AwsOps.BracketOps[F]

  protected def parSequenceOps(implicit contextShift: ContextShift[F] = defaultContextShift): AwsOps.ParSequenceOps[F]
  
  def defaultTimer: Timer[F]
  
  def defaultContextShift: ContextShift[F]
  
  /** Helper to allow performing an IO operation, but ignore the results. */
  protected val ignoreF: F[_] => F[Unit] = fa => functorMonadOps.map(fa)(scala.Function.const(()))

  /** Given a sequence of IO tasks, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    *
    * Optionally, apply a mapping function for each.
    */
  def waitForTasks[A, R](tasks: Seq[F[A]], limit: Int = 5)
                        (mapEach: F[A] => F[R] = ignoreF)(implicit contextShift: ContextShift[F] = defaultContextShift): F[Unit]
  
  object Implicits {
    implicit final class HasFunctorMonadOps[A](fa: F[A]) {
      def map[B](f: A => B): F[B] = functorMonadOps.map(fa)(f)
      def flatMap[B](f: A => F[B]): F[B] = functorMonadOps.flatMap(fa)(f)
    }
    
    implicit final class HasBracketOps[A](fa: F[A]) {
      def bracket[B](use: A => F[B])(release: A => F[Unit]): F[B] = bracketOps.bracket(fa)(use)(release)
    }
    
    implicit final class HasParSequence[A](val tfa: List[F[A]]) {
      def parSequence(implicit contextShift: ContextShift[F] = defaultContextShift): F[List[A]] = parSequenceOps.parSequence(tfa)
    }
  }
}

object AwsOps {
  
  sealed trait ParSequenceOps[F[_]] {
    def parSequence[A](tfa: List[F[A]]): F[List[A]]
  }
  
  sealed trait BracketOps[F[_]] {
    def bracket[A, B](fa: F[A])(use: A => F[B])(release: A => F[Unit]): F[B]
  }
  
  sealed trait FunctorMonadOps[F[_]] {
    def map[A, B](fa: F[A])(f: A => B): F[B]
    def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
  }
  
  implicit object ForId extends AwsOps[Id] {
    override def pure[A](a: => A): Id[A] = a
    
    override def sleep(d: FiniteDuration)(implicit timer: Timer[Id]): Id[Unit] = Thread.sleep(d.toMillis)
    
    override def raiseError(t: Throwable): Id[Nothing] = throw t

    override def defaultTimer: Timer[Id] = null.asInstanceOf[Timer[Id]] //TODO
  
    override def defaultContextShift: ContextShift[Id] = null.asInstanceOf[ContextShift[Id]] //TODO
    
    override protected def parSequenceOps(implicit contextShift: ContextShift[Id] = defaultContextShift): ParSequenceOps[Id] = new ParSequenceOps[Id] {
      override def parSequence[A](tfa: List[Id[A]]): Id[List[A]] = tfa
    }
    
    override protected val functorMonadOps: FunctorMonadOps[Id] = new FunctorMonadOps[Id] {
      override def map[A, B](fa: Id[A])(f: A => B): Id[B] = f(fa)
      override def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = f(fa)
    }
    
    override protected val bracketOps: BracketOps[Id] = new BracketOps[Id] {
      override def bracket[A, B](fa: Id[A])(use: A => Id[B])(release: A => Id[Unit]): Id[B] = {
        try {
          use(fa)
        } finally {
          release(fa)
        }
      }
    }
    
    override def waitForTasks[A, R](tasks: Seq[Id[A]], limit: Int = 5)
                        (mapEach: Id[A] => Id[R] = ignoreF)(implicit contextShift: ContextShift[Id] = defaultContextShift): Id[Unit] = () 
  }
  
  implicit object ForIO extends AwsOps[IO] {
    override def pure[A](a: => A): IO[A] = IO(a)
    
    override def sleep(d: FiniteDuration)(implicit timer: Timer[IO]): IO[Unit] = IO.sleep(d)
    
    override def raiseError(t: Throwable): IO[Nothing] = IO.raiseError(t)
    
    override def defaultTimer: Timer[IO] = org.broadinstitute.dig.aws.Implicits.Defaults.timer
  
    override def defaultContextShift: ContextShift[IO] = org.broadinstitute.dig.aws.Implicits.Defaults.contextShift
    
    override protected def parSequenceOps(implicit contextShift: ContextShift[IO] = defaultContextShift): ParSequenceOps[IO] = new ParSequenceOps[IO] {
      override def parSequence[A](tfa: List[IO[A]]): IO[List[A]] = {
        import cats.implicits._

        tfa.parSequence
      }
    }
    
    override val functorMonadOps: FunctorMonadOps[IO] = new FunctorMonadOps[IO] {
      override def map[A, B](fa: IO[A])(f: A => B): IO[B] = fa.map(f)
      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
    }
    
    override protected val bracketOps: BracketOps[IO] = new BracketOps[IO] {
      override def bracket[A, B](fa: IO[A])(use: A => IO[B])(release: A => IO[Unit]): IO[B] = {
        fa.bracket(use)(release)
      }
    }
    
    override def waitForTasks[A, R](tasks: Seq[IO[A]], limit: Int = 5)
                        (mapEach: IO[A] => IO[R] = ignoreF)(implicit contextShift: ContextShift[IO] = defaultContextShift): IO[Unit] = Utils.waitForTasks(tasks, limit)(mapEach) 
  }
}
