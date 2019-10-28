package org.broadinstitute.dig.aws

import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.ReleaseLabel
import org.broadinstitute.dig.aws.config.emr.RoleId
import org.broadinstitute.dig.aws.config.emr.SecurityGroupId
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * Sep 17, 2019
 */
final class AWSConfigTest extends FunSuite {
  import org.broadinstitute.dig.aws.config.AWSConfig.fromTypesafeConfig
  
  test("fromTypesafeConfig - bad input") {
    assert(fromTypesafeConfig(ConfigFactory.empty(), "foo").isFailure)
    assert(fromTypesafeConfig(ConfigFactory.parseString("{ }"), "foo").isFailure)
    assert(fromTypesafeConfig(ConfigFactory.parseString("{ foo = { bar = { } } }"), "foo.bar").isFailure)
  }
  
  test("fromTypesafeConfig - good input, no defaults") {
    val bucket = "some-bucket"
    val sshKeyName = "some-ssh-key-name"
    val subnetId = "subnet-some-subnet-id"
    val releaseLabel = "emr-some-release-label"
    val securityGroupIds = Seq("sg-0", "sg-1")
    val serviceRoleId = "some-role-id"
    val jobFlowRoleId = "some-other-id"
    val autoScalingRoleId = "yet-another-role-id"
    
    val confString = s"""
    {
      foo {
        bar {
          aws {
            s3 { bucket = "$bucket" }
            emr {
              sshKeyName = "$sshKeyName"
              subnetId = "$subnetId"
              releaseLabel = "$releaseLabel"
              securityGroupIds = ["sg-0", "sg-1"]
              serviceRoleId = "$serviceRoleId"
              jobFlowRoleId = "$jobFlowRoleId"
              autoScalingRoleId = "$autoScalingRoleId"
            }
          }
        }
      }
    }"""
              
    val parsed = fromTypesafeConfig(ConfigFactory.parseString(confString), "foo.bar.aws").get
    
    val expected = AWSConfig(
        s3 = S3Config(bucket),
        emr = EmrConfig(
            sshKeyName = sshKeyName,
            subnetId = SubnetId(subnetId),
            releaseLabel = ReleaseLabel(releaseLabel),
            securityGroupIds = securityGroupIds.map(SecurityGroupId(_)),
            serviceRoleId = RoleId(serviceRoleId),
            jobFlowRoleId = RoleId(jobFlowRoleId),
            autoScalingRoleId = RoleId(autoScalingRoleId)))
             
    assert(parsed === expected)
  }
  
  test("fromTypesafeConfig - good input, with defaults") {
    val bucket = "some-bucket"
    val sshKeyName = "some-ssh-key-name"
    val subnetId = "subnet-some-subnet-id"
    val releaseLabel = "emr-some-release-label"
    val securityGroupIds = Seq("sg-0", "sg-1")
    val serviceRoleId = "some-role-id"
    val jobFlowRoleId = "some-other-id"
    val autoScalingRoleId = "yet-another-role-id"
    
    val confString = s"""
    {
      foo {
        bar {
          aws {
            s3 { bucket = "$bucket" }
            emr {
              sshKeyName = "$sshKeyName"
              subnetId = "$subnetId"
            }
          }
        }
      }
    }"""
              
    val parsed = fromTypesafeConfig(ConfigFactory.parseString(confString), "foo.bar.aws").get
    
    val expected = AWSConfig(
        s3 = S3Config(bucket),
        emr = EmrConfig(
            sshKeyName = sshKeyName,
            subnetId = SubnetId(subnetId),
            releaseLabel = ReleaseLabel.emrLatest,
            securityGroupIds = Nil,
            serviceRoleId = RoleId.defaultRole,
            jobFlowRoleId = RoleId.ec2DefaultRole,
            autoScalingRoleId = RoleId.autoScalingDefaultRole))
             
    assert(parsed === expected)
  }
}
