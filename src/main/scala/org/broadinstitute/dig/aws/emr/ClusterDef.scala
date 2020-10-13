package org.broadinstitute.dig.aws.emr

import org.broadinstitute.dig.aws.Ec2.Strategy
import org.broadinstitute.dig.aws.emr.configurations.Configuration
import software.amazon.awssdk.services.emr.model.EbsBlockDeviceConfig
import software.amazon.awssdk.services.emr.model.EbsConfiguration
import software.amazon.awssdk.services.emr.model.InstanceGroupConfig
import software.amazon.awssdk.services.emr.model.InstanceRoleType
import software.amazon.awssdk.services.emr.model.VolumeSpecification

/** Parameterized configuration for an EMR cluster. Constant settings are
  * located in `config.emr.EmrConfig` and are loaded in the JSON.
  */
final case class ClusterDef(
    name: String,
    amiId: Option[AmiId] = None, //AmiId.amazonLinux_2018_3,
    releaseLabel: ReleaseLabel = ReleaseLabel.emrDefault,
    instances: Int = 3,
    masterInstanceType: Strategy = Strategy.default,
    slaveInstanceType: Strategy = Strategy.default,
    masterVolumeSizeInGB: Int = 32,
    slaveVolumeSizeInGB: Int = 32,
    applications: Seq[ApplicationName] = ClusterDef.defaultApplications,
    applicationConfigurations: Seq[Configuration] = Seq.empty,
    bootstrapScripts: Seq[BootstrapScript] = Seq.empty,
    bootstrapSteps: Seq[Job.Step] = Seq.empty,
    visibleToAllUsers: Boolean = true,
    stepConcurrency: Integer = 1
) {
  require(name.matches("[A-Za-z_]+[A-Za-z0-9_]*"), s"Illegal cluster name: $name")
  require(instances >= 1)

  /** Instance configuration for the master node. */
  lazy val masterInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = VolumeSpecification.builder
      .sizeInGB(masterVolumeSizeInGB)
      .volumeType("gp2")
      .build

    val deviceConfig = EbsBlockDeviceConfig.builder.volumeSpecification(volumeSpec).build
    val ebsConfig    = EbsConfiguration.builder.ebsBlockDeviceConfigs(deviceConfig).build

    InstanceGroupConfig.builder
      .ebsConfiguration(ebsConfig)
      .instanceType(masterInstanceType.instanceType.toString)
      .instanceRole(InstanceRoleType.MASTER)
      .instanceCount(1)
      .build
  }

  /** Instance configuration for the slave nodes. */
  lazy val slaveInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = VolumeSpecification.builder
      .sizeInGB(slaveVolumeSizeInGB)
      .volumeType("gp2")
      .build

    val deviceConfig = EbsBlockDeviceConfig.builder.volumeSpecification(volumeSpec).build
    val ebsConfig    = EbsConfiguration.builder.ebsBlockDeviceConfigs(deviceConfig).build

    InstanceGroupConfig.builder
      .ebsConfiguration(ebsConfig)
      .instanceType(slaveInstanceType.instanceType.toString)
      .instanceRole(InstanceRoleType.CORE)
      .instanceCount(instances - 1)
      .build
  }

  /** Sequence of all instance groups used to create this cluster. */
  lazy val instanceGroups: Seq[InstanceGroupConfig] = {
    Seq(masterInstanceGroupConfig, slaveInstanceGroupConfig)
      .filter(_.instanceCount > 0)
  }
}

/** Companion object for creating an EMR cluster. */
object ClusterDef {

  /** The default set used by pretty much every cluster. */
  val defaultApplications: Seq[ApplicationName] = Seq(
    ApplicationName.hadoop,
    ApplicationName.spark,
    ApplicationName.hive,
    ApplicationName.hue
  )
}
