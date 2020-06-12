package org.broadinstitute.dig.aws

/**
 * @author clint
 * Jul 27, 2018
 * Jun 9, 2020
 */
final class EmrTest extends AwsFunSuite {
  /**
   * Create a cluster and run a simple script job.
   */
  testWithCluster("Simple Cluster", "test_script.py")
}
