package org.broadinstitute.dig.aws.emr

import org.broadinstitute.dig.aws.JobStep

import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification

/** Parameterized configuration for an EMR cluster. Constant settings are
  * located in `config.emr.EmrConfig` and are loaded in the JSON.
  */
final case class Cluster(
    name: String,
    amiId: Option[AmiId] = None, //AmiId.amazonLinux_2018_3,
    instances: Int = 3,
    masterInstanceType: InstanceType = InstanceType.m5_4xlarge,
    slaveInstanceType: InstanceType = InstanceType.m5_2xlarge,
    masterVolumeSizeInGB: Int = 32,
    slaveVolumeSizeInGB: Int = 32,
    applications: Seq[ApplicationName] = Cluster.defaultApplications,
    configurations: Seq[ApplicationConfig] = Cluster.defaultConfigurations,
    bootstrapScripts: Seq[BootstrapScript] = Seq.empty,
    bootstrapSteps: Seq[JobStep] = Seq.empty,
    keepAliveWhenNoSteps: Boolean = false,
    visibleToAllUsers: Boolean = true
) {
  require(name.matches("[A-Za-z_]+[A-Za-z0-9_]*"), s"Illegal cluster name: $name")
  require(instances >= 1)

  /** Instance configuration for the master node. */
  val masterInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = new VolumeSpecification()
      .withSizeInGB(masterVolumeSizeInGB)
      .withVolumeType("gp2")

    val deviceConfig = new EbsBlockDeviceConfig().withVolumeSpecification(volumeSpec)
    val ebsConfig    = new EbsConfiguration().withEbsBlockDeviceConfigs(deviceConfig)

    new InstanceGroupConfig()
      .withEbsConfiguration(ebsConfig)
      .withInstanceType(masterInstanceType.value)
      .withInstanceRole(InstanceRoleType.MASTER)
      .withInstanceCount(1)
  }

  /** Instance configuration for the slave nodes. */
  val slaveInstanceGroupConfig: InstanceGroupConfig = {
    val volumeSpec = new VolumeSpecification()
      .withSizeInGB(slaveVolumeSizeInGB)
      .withVolumeType("gp2")

    val deviceConfig = new EbsBlockDeviceConfig().withVolumeSpecification(volumeSpec)
    val ebsConfig    = new EbsConfiguration().withEbsBlockDeviceConfigs(deviceConfig)

    new InstanceGroupConfig()
      .withEbsConfiguration(ebsConfig)
      .withInstanceType(slaveInstanceType.value)
      .withInstanceRole(InstanceRoleType.CORE)
      .withInstanceCount(instances - 1)
  }

  /** Sequence of all instance groups used to create this cluster. */
  val instanceGroups: Seq[InstanceGroupConfig] = {
    Seq(masterInstanceGroupConfig, slaveInstanceGroupConfig)
      .filter(_.getInstanceCount > 0)
  }
}

/** Companion object for creating an EMR cluster. */
object Cluster {

  /** The default set used by pretty much every cluster. */
  val defaultApplications: Seq[ApplicationName] = Seq(
    ApplicationName.hadoop,
    ApplicationName.spark,
    ApplicationName.hive,
    ApplicationName.pig,
    ApplicationName.hue
  )

  /** The default configurations for applications. */
  val defaultConfigurations: Seq[ApplicationConfig] = Seq()
}
