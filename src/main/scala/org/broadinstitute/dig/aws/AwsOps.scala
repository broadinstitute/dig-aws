package org.broadinstitute.dig.aws

import scala.language.higherKinds

import scala.concurrent.duration.FiniteDuration

import cats.Id
import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Timer

/**
 * @author clint
 * Oct 1, 2019
 */
sealed trait AwsOps[F[_]] {
  def pure[A](a: => A): F[A]
  
  def sleep(d: FiniteDuration)(implicit timer: Timer[F]): F[Unit]
  
  def raiseError[A](t: Throwable): F[A] 
  
  protected def fOps: AwsOps.FOps[F]

  protected def sequenceOps(implicit contextShift: ContextShift[F] = defaultContextShift): AwsOps.SequenceOps[F]
  
  def defaultTimer: Timer[F]
  
  def defaultContextShift: ContextShift[F]
  
  /** Helper to allow performing an IO operation, but ignore the results. */
  protected val ignoreF: F[_] => F[Unit] = fa => fOps.map(fa)(scala.Function.const(()))

  /** Given a sequence of IO tasks, run them in parallel, but limit the maximum
    * concurrency so too many clusters aren't created at once.
    *
    * Optionally, apply a mapping function for each.
    */
  def waitForTasks[A, R](tasks: Seq[F[A]], limit: Int = 5)
                        (mapEach: F[A] => F[R] = ignoreF)
                        (implicit contextShift: ContextShift[F] = defaultContextShift): F[Unit]
  
  object Implicits {
    implicit final class HasFOps[A](fa: F[A]) {
      def map[B](f: A => B): F[B] = fOps.map(fa)(f)
      def flatMap[B](f: A => F[B]): F[B] = fOps.flatMap(fa)(f)
      
      def bracket[B](use: A => F[B])(release: A => F[Unit]): F[B] = fOps.bracket(fa)(use)(release)
      
      def run(): A = fOps.run(fa)
    }
    
    implicit final class HasSequenceOps[A](val tfa: List[F[A]]) {
      def sequence: F[List[A]] = sequenceOps().sequence(tfa)
      
      def parSequence(implicit contextShift: ContextShift[F] = defaultContextShift): F[List[A]] = {
        sequenceOps.parSequence(tfa)
      }
    }
  }
}

object AwsOps {
  
  sealed trait FOps[F[_]] {
    def map[A, B](fa: F[A])(f: A => B): F[B]
    def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
    
    def bracket[A, B](fa: F[A])(use: A => F[B])(release: A => F[Unit]): F[B]
    
    def run[A](fa: F[A]): A
  }
  
  sealed trait SequenceOps[F[_]] {
    def sequence[A](tfa: List[F[A]]): F[List[A]]
    
    def parSequence[A](tfa: List[F[A]]): F[List[A]]
  }
  
  implicit object ForId extends AwsOps[Id] {
    override def pure[A](a: => A): Id[A] = a
    
    override def sleep(d: FiniteDuration)(implicit timer: Timer[Id]): Id[Unit] = Thread.sleep(d.toMillis)
    
    override def raiseError[A](t: Throwable): Id[A] = throw t

    override def defaultTimer: Timer[Id] = null.asInstanceOf[Timer[Id]] //TODO
  
    override def defaultContextShift: ContextShift[Id] = null.asInstanceOf[ContextShift[Id]] //TODO
    
    override protected def sequenceOps(
        implicit contextShift: ContextShift[Id] = defaultContextShift): SequenceOps[Id] = new SequenceOps[Id] {
      
      override def sequence[A](ids: List[Id[A]]): Id[List[A]] = ids
      
      override def parSequence[A](ids: List[Id[A]]): Id[List[A]] = ids
    }
    
    override protected val fOps: FOps[Id] = new FOps[Id] {
      override def map[A, B](fa: Id[A])(f: A => B): Id[B] = f(fa)
      override def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = f(fa)
      
      override def bracket[A, B](fa: Id[A])(use: A => Id[B])(release: A => Id[Unit]): Id[B] = {
        try {
          use(fa)
        } finally {
          release(fa)
        }
      }
      
      override def run[A](fa: Id[A]): A = fa
    }
    
    override def waitForTasks[A, R](
        tasks: Seq[Id[A]], 
        limit: Int = 5)
        (mapEach: Id[A] => Id[R] = ignoreF)
        (implicit contextShift: ContextShift[Id] = defaultContextShift): Id[Unit] = () 
  }
  
  implicit object ForIO extends AwsOps[IO] {
    override def pure[A](a: => A): IO[A] = IO(a)
    
    override def sleep(d: FiniteDuration)(implicit timer: Timer[IO]): IO[Unit] = IO.sleep(d)
    
    override def raiseError[A](t: Throwable): IO[A] = IO.raiseError(t)
    
    override def defaultTimer: Timer[IO] = org.broadinstitute.dig.aws.Implicits.Defaults.timer
  
    override def defaultContextShift: ContextShift[IO] = org.broadinstitute.dig.aws.Implicits.Defaults.contextShift
    
    override protected def sequenceOps(
        implicit contextShift: ContextShift[IO] = defaultContextShift): SequenceOps[IO] = new SequenceOps[IO] {
      
      import cats.implicits._
      
      override def sequence[A](ios: List[IO[A]]): IO[List[A]] = ios.sequence
      
      override def parSequence[A](ios: List[IO[A]]): IO[List[A]] = ios.parSequence
    }
    
    override protected val fOps: FOps[IO] = new FOps[IO] {
      override def map[A, B](fa: IO[A])(f: A => B): IO[B] = fa.map(f)
      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
      
      override def bracket[A, B](fa: IO[A])(use: A => IO[B])(release: A => IO[Unit]): IO[B] = fa.bracket(use)(release)
      
      override def run[A](fa: IO[A]): A = fa.unsafeRunSync()
    }
    
    override def waitForTasks[A, R](
        tasks: Seq[IO[A]], 
        limit: Int = 5)
        (mapEach: IO[A] => IO[R] = ignoreF)
        (implicit contextShift: ContextShift[IO] = defaultContextShift): IO[Unit] = {
      
      Utils.waitForTasks(tasks, limit)(mapEach) 
    }
  }
}
