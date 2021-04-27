package org.broadinstitute.dig.aws.config

import org.broadinstitute.dig.aws.config.emr._
import org.json4s._

/** AWS EMR settings for creating a job cluster. These remain constant across
  * all clusters created: subnet, SSH keys, EMR release, roles, etc.
  */
final case class EmrConfig(
    sshKeyName: String,
    subnetIds: Seq[SubnetId],
    securityGroupIds: Seq[SecurityGroupId] = Seq.empty,
    serviceRoleId: RoleId = RoleId.defaultRole,
    jobFlowRoleId: RoleId = RoleId.ec2DefaultRole,
    autoScalingRoleId: RoleId = RoleId.autoScalingDefaultRole
) {
  require(subnetIds.nonEmpty)
}

/** Companion object with custom JSON serializers.
  */
object EmrConfig {

  /** Custom JSON serializers for various EMR case class settings. To use
    * these when de-serializing, add them like so:
    *
    * implicit val formats = json4s.DefaultFormats ++ EmrConfig.customSerializers
    */
  val customSerializers: Seq[CustomSerializer[_]] = Seq(
    RoleId.Serializer,
    SecurityGroupId.Serializer,
    SubnetId.Serializer
  )
}
